package registry

import (
	"encoding/json"
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

type WorkerStream struct {
	workerInfo  workerInfo
	stream      transport.ExchangeStream
	lastSeen    time.Time
	activeTasks int
	registry    *WorkerRegistry
}

func (w *WorkerStream) GetID() string {
	return w.workerInfo.ID
}

func (w *WorkerStream) UpdateCapabilities(update models.WorkerCapabilitiesUpdate) {
	w.workerInfo.UpdateCapabilities(update)
}

func (w *WorkerStream) GetSchemaForCapability(capability string) (schema.StepSchemas, bool) {
	return w.workerInfo.GetSchemaForCapability(capability)
}

func (w *WorkerStream) ProcessStream(stream transport.ExchangeStream) bool {
	if stream == nil {
		return false
	}
	w.stream = stream

	// Listen for task execution results and administrative acknowledgements from the worker in a separate goroutine.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// TODO: check if needed recover here
				// Prevent crashes from uninitialized mock streams in unit tests
			}
		}()

		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			if resp == nil {
				logger.L().Warn("Received nil response from worker", logger.String("worker_id", w.workerInfo.ID))
				continue
			}

			// Intercept and route control signaling messages (such as shared memory purge notifications and step execution logs).
			if len(resp.AppMetadata) > 0 {
				var ctrl models.ControlMessage
				if err := json.Unmarshal(resp.AppMetadata, &ctrl); err == nil {
					if ctrl.Type == "step-log" && ctrl.LogData != nil {
						if w.registry != nil {
							if clientID, ok := w.registry.GetClientIDForWorkflow(ctrl.LogData.WorkflowID); ok {
								if clientStream, ok := w.registry.GetActiveClientStream(clientID); ok {
									_ = clientStream.Send(&transport.FlightData{
										DataBody: []byte("LOG:" + ctrl.LogData.Text),
									})
								}
							}
						}
						continue
					}
				}
				logger.L().Info("Received control signaling message", logger.Any("metadata", resp.AppMetadata))
				continue
			}

			var result models.TaskResult
			if err := json.Unmarshal(resp.DataBody, &result); err != nil {
				logger.L().Warn("Failed to unmarshal result", logger.Error(err))
			} else if w.registry != nil {
				w.registry.RouteResult(result)
			}

		}
	}()

	return true
}

func (w *WorkerStream) StopStream() {
	w.stream = nil
}

func (w *WorkerStream) LastSeen() {
	w.lastSeen = time.Now()
}
