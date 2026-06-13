package registry

import (
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/schema"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

type workerInfo struct {
	mu           sync.RWMutex
	ID           string
	Registration models.WorkerRegistration
	Capabilities []string
	Schemas      map[string]schema.StepSchemas
}

func (w *workerInfo) UpdateCapabilities(update models.WorkerCapabilitiesUpdate) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Merge unique capabilities and update schemas
	capsMap := make(map[string]bool)
	for _, c := range w.Capabilities {
		capsMap[c] = true
	}
	for _, c := range update.Capabilities {
		if !capsMap[c] {
			w.Capabilities = append(w.Capabilities, c)
			capsMap[c] = true
		}
	}

	// Update schemas
	if w.Schemas == nil {
		w.Schemas = make(map[string]schema.StepSchemas)
	}
	maps.Copy(w.Schemas, update.Schemas)
}

func (w *workerInfo) GetSchemaForCapability(capability string) (schema.StepSchemas, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.Schemas == nil {
		return schema.StepSchemas{}, false
	}
	s, ok := w.Schemas[capability]
	return s, ok
}

func (w *workerInfo) SupportsCapability(capability string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, c := range w.Capabilities {
		if c == capability {
			return true
		}
	}
	return false
}

type NodeType string

const (
	NodeTypeWorker NodeType = "worker"
	NodeTypeClient NodeType = "client"
)

type NodeStream struct {
	mu          sync.RWMutex
	ID          string
	Type        NodeType
	workerInfo  workerInfo
	stream      transport.ExchangeStream
	lastSeen    time.Time
	activeTasks int
	registry    *NodeRegistry
	results     *shardedResultMap
}

func (w *NodeStream) GetID() string {
	return w.ID
}

func (w *NodeStream) UpdateCapabilities(update models.WorkerCapabilitiesUpdate) {
	w.workerInfo.UpdateCapabilities(update)
}

func (w *NodeStream) GetSchemaForCapability(capability string) (schema.StepSchemas, bool) {
	return w.workerInfo.GetSchemaForCapability(capability)
}

func (w *NodeStream) ProcessStream(stream transport.ExchangeStream) <-chan error {
	w.mu.Lock()
	w.stream = stream
	w.mu.Unlock()

	errChan := make(chan error, 1)

	if w.Type == NodeTypeClient {
		// Clients stream logs and prompts TO the client.
		// For interactive/debug, the orchestrator reads directly from the stream.
		// We just wait for the connection to close. The control_plane DoExchange loop
		// listens to ctx.Done() and will handle the disconnect.
		return errChan
	}

	// Listen for task execution results and administrative acknowledgements from the worker in a separate goroutine.
	go func() {
		defer close(errChan)
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic in stream reader: %v", r)
				logger.L().Error("stream error: panic in stream reader",
					logger.Component("control-plane"),
					logger.Error(err),
				)
				errChan <- err
			}
		}()

		for {
			resp, err := stream.Recv()
			if err != nil {
				errChan <- err
				return
			}
			if resp == nil {
				logger.L().Warn("worker stream anomaly: received nil response from worker",
					logger.Component("control-plane"),
					logger.WorkerID(w.workerInfo.ID),
				)
				continue
			}

			// Intercept and route control signaling messages (such as shared memory purge notifications and step execution logs).
			if len(resp.AppMetadata) > 0 {
				var ctrl models.ControlMessage
				if err := json.Unmarshal(resp.AppMetadata, &ctrl); err == nil {
					if ctrl.Type == "step-log" && ctrl.LogData != nil {
						if w.registry != nil {
							if clientID, ok := w.registry.GetClientIDForWorkflow(ctrl.LogData.WorkflowID); ok {
								if clientStream, ok := w.registry.GetNode(clientID); ok {
									if err := clientStream.Send(&transport.FlightData{
										DataBody: []byte("LOG:" + ctrl.LogData.Text),
									}); err != nil {
										logger.L().Warn("log routing failed: unable to send log data to client",
											logger.Component("control-plane"),
											logger.ClientID(clientID),
											logger.Error(err),
										)
									}
								}
							}
						}
						continue
					}
					if ctrl.Type == models.ActionRequestFile && ctrl.FileRequest != nil {
						if w.registry != nil {
							if clientID, ok := w.registry.GetClientIDForWorkflow(ctrl.FileRequest.WorkflowID); ok {
								if clientStream, ok := w.registry.GetNode(clientID); ok {
									// Inject worker address before relaying
									ctrl.FileRequest.WorkerAddress = w.workerInfo.Registration.Address
									ctrlBody, _ := json.Marshal(ctrl)
									if err := clientStream.Send(&transport.FlightData{
										AppMetadata: ctrlBody,
									}); err != nil {
										logger.L().Warn("file request routing failed: unable to send request to client",
											logger.Component("control-plane"),
											logger.ClientID(clientID),
											logger.Error(err),
										)
									}
								}
							}
						}
						continue
					}
				}
				logger.L().Info("control signal received: processed administrative message from node",
					logger.Component("control-plane"),
					logger.Any("metadata", resp.AppMetadata),
				)
				continue
			}

			if w.Type == NodeTypeWorker {
				var result models.TaskResult
				if err := json.Unmarshal(resp.DataBody, &result); err != nil {
					logger.L().Warn("result processing failed: unable to unmarshal task result",
						logger.Component("control-plane"),
						logger.Error(err),
						logger.WorkerID(w.workerInfo.ID),
					)
				} else {
					w.RouteResult(result)
				}
			}

		}
	}()

	return errChan
}

func (w *NodeStream) StopStream() {
	w.mu.Lock()
	w.stream = nil
	w.mu.Unlock()

	if w.results != nil {
		w.results.clearAndCloseAll(models.TaskResult{
			Status:       models.TaskStatusFailed,
			ErrorMessage: "worker disconnected",
		})
	}
}

func (w *NodeStream) LastSeen() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastSeen = time.Now()
}

func (w *NodeStream) GetStream() transport.ExchangeStream {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.stream
}

func (w *NodeStream) GetLastSeen() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lastSeen
}

func (w *NodeStream) GetActiveTasks() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.activeTasks
}

func (w *NodeStream) UpdateHeartbeat(load int, t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.activeTasks = load
	w.lastSeen = t
}

func (w *NodeStream) SupportsCapability(capability string) bool {
	return w.workerInfo.SupportsCapability(capability)
}

func (w *NodeStream) RegisterResultFuture(taskID string, future *models.TaskFuture) {
	if w.results != nil {
		w.results.set(taskID, future)
	}
}

func (w *NodeStream) DeregisterResultFuture(taskID string) {
	if w.results != nil {
		w.results.delete(taskID)
	}
}

func (w *NodeStream) RouteResult(result models.TaskResult) bool {
	if w.results == nil {
		return false
	}
	future, ok := w.results.get(result.TaskID)
	if !ok {
		return false
	}
	return future.Resolve(result)
}

func (w *NodeStream) Send(data *transport.FlightData) error {
	w.mu.RLock()
	stream := w.stream
	w.mu.RUnlock()
	if stream == nil {
		return fmt.Errorf("stream is closed")
	}
	return stream.Send(data)
}
