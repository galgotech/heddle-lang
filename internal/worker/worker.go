package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/schema"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

type Worker struct {
	id           string
	transport    transport.Client
	capabilities map[string][]string
	schemas      map[string]map[string]schema.StepSchemas
	pluginServer *PluginServer
	Ready        chan struct{}
	readyOnce    sync.Once
}

func NewWorker(transport transport.Client, pluginServer *PluginServer) (*Worker, error) {
	worker := &Worker{
		pluginServer: pluginServer,
		transport:    transport,

		id:           "worker-" + uuid.New().String()[:8],
		capabilities: make(map[string][]string),
		schemas:      make(map[string]map[string]schema.StepSchemas),
		Ready:        make(chan struct{}),
	}

	return worker, nil
}

func (w *Worker) GetID() string {
	return w.id
}

func (w *Worker) Start(ctx context.Context) error {
	ctx = metadata.AppendToOutgoingContext(ctx, "worker-id", w.GetID())

	// 1. Register with Control Plane
	reg := models.WorkerRegistration{
		Address: "localhost", // Should be actual address
	}
	body, err := json.Marshal(reg)
	if err != nil {
		return fmt.Errorf("failed to marshal registration: %w", err)
	}
	res, err := w.transport.DoAction(ctx, &transport.Action{
		Type: models.ActionRegisterWorker,
		Body: body,
	})
	if err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}
	if _, err := res.Recv(); err != nil {
		return fmt.Errorf("failed to receive registration result: %w", err)
	}
	logger.L().Info("Worker registered with control plane", logger.String("id", w.GetID()))

	// 2. Start Heartbeats
	go w.startHeartbeat(ctx)

	// 3. watch plugin registrations
	go w.watchPluginRegistrations(ctx)

	// 4. Start Plugin Server (UDS)
	go func() {
		if err := w.pluginServer.Start(ctx); err != nil {
			logger.L().Error("Plugin server error", logger.Error(err))
		}
	}()
	<-w.pluginServer.Ready

	// 4. run task loop

	go func() {
		err := w.run(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || ctx.Err() != nil {
				logger.L().Info("Worker task loop stopped", logger.Error(err))
				return
			}
			logger.L().Fatal("Worker run loop exited with error", logger.Error(err))
		}
	}()

	// 5. watch for shutdown
	go func() {
		<-ctx.Done()
		logger.L().Info("Worker shutting down, sending deregistration", logger.String("id", w.GetID()))
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownCtx = metadata.AppendToOutgoingContext(shutdownCtx, "worker-id", w.GetID())
		_, _ = w.transport.DoAction(shutdownCtx, &transport.Action{
			Type: models.ActionDeregisterWorker,
			Body: []byte("{}"),
		})
	}()

	return nil
}

func (w *Worker) startHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hb := models.WorkerHeartbeat{
				Timestamp: time.Now(),
				Load:      0, // TODO: Track actual load
			}
			body, _ := json.Marshal(hb)
			_, err := w.transport.DoAction(ctx, &transport.Action{
				Type: models.ActionHeartbeat,
				Body: body,
			})
			if err != nil {
				logger.L().Error("Heartbeat failed", logger.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) watchPluginRegistrations(ctx context.Context) {
	for reg := range w.pluginServer.PluginSyncRegiter() {
		w.capabilities[reg.Namespace] = slices.Concat(slices.Sorted(maps.Keys(reg.Schemas)), slices.Sorted(maps.Keys(reg.Resources)))
		w.schemas[reg.Namespace] = reg.Schemas

		cabalities := []string{}
		for _, caps := range w.capabilities {
			cabalities = append(cabalities, caps...)
		}

		schemas := make(map[string]schema.StepSchemas)
		for _, sch := range w.schemas {
			maps.Copy(schemas, sch)
		}

		// Notify Control Plane
		update := models.WorkerCapabilitiesUpdate{
			Capabilities: cabalities,
			Schemas:      schemas,
		}
		body, err := json.Marshal(update)
		if err != nil {
			logger.L().Error("failed to marshal capabilities update: %w", logger.Error(err))
			continue
		}

		logger.L().Info("Sending update to control plane", logger.Strings("capabilities", cabalities))
		res, err := w.transport.DoAction(ctx, &transport.Action{
			Type: models.ActionUpdateCapabilities,
			Body: body,
		})

		logger.L().Info("Sent update to control plane")
		if err != nil {
			logger.L().Error("failed to update capabilities: %w", logger.Error(err))
			continue
		}
		if _, err := res.Recv(); err != nil {
			logger.L().Error("failed to receive update result: %w", logger.Error(err))
			continue
		}

		logger.L().Info("Worker capabilities updated", logger.Strings("capabilities", cabalities))
	}
}

func (w *Worker) run(ctx context.Context) error {
	stream, err := w.transport.DoExchange(ctx)
	if err != nil {
		return fmt.Errorf("failed to start exchange stream: %w", err)
	}

	// Signal that the worker is ready to receive tasks
	w.readyOnce.Do(func() {
		close(w.Ready)
	})

	logger.L().Info("Worker task loop started", logger.String("id", w.GetID()))
	for {
		data, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("task stream closed: %w", err)
		}

		// Check AppMetadata for control messages (e.g., workflow purge)
		if len(data.AppMetadata) > 0 {
			var ctrl models.ControlMessage
			if err := json.Unmarshal(data.AppMetadata, &ctrl); err == nil {
				if ctrl.Type == models.ActionPurgeWorkflow && ctrl.PurgeData != nil {
					logger.L().Info("SHM purged for workflow", logger.String("id", ctrl.PurgeData.WorkflowID))

					// Send Acknowledgment
					ack := models.ControlMessage{
						Type: models.ActionPurgeAck,
						PurgeAck: &models.PurgeAck{
							WorkflowID: ctrl.PurgeData.WorkflowID,
							WorkerID:   w.GetID(),
						},
					}
					ackBody, _ := json.Marshal(ack)
					if err := stream.Send(&transport.FlightData{AppMetadata: ackBody}); err != nil {
						logger.L().Error("Failed to send purge ack", logger.Error(err))
					}
				}
			}
			continue // control messages carry no DataBody
		}

		var task models.StepExecutionTask
		if err := json.Unmarshal(data.DataBody, &task); err != nil {
			logger.L().Error("Failed to unmarshal task", logger.Error(err))
			continue
		}

		logger.L().Info("Received task", logger.String("id", task.TaskID), logger.String("step", task.Step.Call[1]))

		go func(t models.StepExecutionTask) {
			var result models.TaskResult
			var err error

			logWriter := &workerLogWriter{
				workflowID: t.WorkflowID,
				taskID:     t.TaskID,
				send:       stream.Send,
			}
			taskCtx := plugin.WithOutputWriter(ctx, logWriter)

			result, err = w.pluginServer.DispatchTask(taskCtx, t)

			if err != nil {
				logger.L().Error("Failed to execute task", logger.Error(err))
				// Send failure result back
				result = models.TaskResult{
					TaskID:       t.TaskID,
					Status:       models.TaskStatusFailed,
					ErrorMessage: err.Error(),
				}
			}
			respBody, _ := json.Marshal(result)
			if err := stream.Send(&transport.FlightData{DataBody: respBody}); err != nil {
				logger.L().Error("Failed to send task result", logger.Error(err))
			}
		}(task)
	}
}

type workerLogWriter struct {
	workflowID string
	taskID     string
	send       func(msg *transport.FlightData) error
}

func (w *workerLogWriter) Write(p []byte) (n int, err error) {
	logMsg := models.ControlMessage{
		Type: "step-log",
		LogData: &models.LogData{
			WorkflowID: w.workflowID,
			TaskID:     w.taskID,
			Text:       string(p),
		},
	}
	body, err := json.Marshal(logMsg)
	if err != nil {
		return 0, err
	}
	err = w.send(&transport.FlightData{AppMetadata: body})
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
