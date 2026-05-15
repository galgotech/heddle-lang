package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/internal/worker/internal"
	"github.com/galgotech/heddle-lang/internal/worker/std"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

type Worker struct {
	ID                   string
	ControlPlane         flight.Client
	SocketPath           string
	Capabilities         []string
	Schemas              map[string]schema.StepSchemas
	PluginServer         *PluginServer
	updateCapabilitiesCh chan func(context.Context)
	Ready                chan struct{}
	Registry             *locality.DataLocalityRegistry
}

func (w *Worker) Start(ctx context.Context) error {
	ctx = metadata.AppendToOutgoingContext(ctx, "worker-id", w.ID)

	go w.run(ctx)

	// 1. Register with Control Plane
	reg := models.WorkerRegistration{
		Address: "localhost", // Should be actual address
	}
	body, _ := json.Marshal(reg)
	res, err := w.ControlPlane.DoAction(ctx, &flight.Action{
		Type: models.ActionRegisterWorker,
		Body: body,
	})
	if err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}
	if _, err := res.Recv(); err != nil {
		return fmt.Errorf("failed to receive registration result: %w", err)
	}
	logger.L().Info("Worker registered with control plane", zap.String("id", w.ID))

	// 2. Start Heartbeats
	go w.startHeartbeat(ctx)

	// 2.1 Sync internal capabilities
	internalCaps := make([]string, 0)
	for k := range internal.Registry {
		internalCaps = append(internalCaps, "__internal."+k)
	}
	for k := range std.Registry {
		internalCaps = append(internalCaps, k)
	}

	if err := w.updateCapabilities(ctx, internalCaps, nil, true); err != nil {
		return fmt.Errorf("failed to sync internal capabilities: %w", err)
	}

	// 3. Start Plugin Server (UDS)
	if w.PluginServer == nil {
		w.PluginServer = NewPluginServer(w.SocketPath)
	}
	w.PluginServer.Registry = w.Registry
	w.PluginServer.OnCapabilitiesUpdate = w.UpdateCapabilities
	go func() {
		if err := w.PluginServer.Start(ctx); err != nil {
			logger.L().Error("Plugin server error", zap.Error(err))
		}
	}()
	<-w.PluginServer.Ready

	// 4. Start Task Loop
	return w.startTaskLoop(ctx)
}

func (w *Worker) run(ctx context.Context) {
	for {
		select {
		case fn := <-w.updateCapabilitiesCh:
			fn(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) startHeartbeat(ctx context.Context) {
	ctx = metadata.AppendToOutgoingContext(ctx, "worker-id", w.ID)
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
			_, err := w.ControlPlane.DoAction(ctx, &flight.Action{
				Type: models.ActionHeartbeat,
				Body: body,
			})
			if err != nil {
				logger.L().Error("Heartbeat failed", zap.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) startTaskLoop(ctx context.Context) error {
	ctx = metadata.AppendToOutgoingContext(ctx, "worker-id", w.ID)
	stream, err := w.ControlPlane.DoExchange(ctx)
	if err != nil {
		return fmt.Errorf("failed to start exchange stream: %w", err)
	}

	if w.Ready != nil {
		close(w.Ready)
		w.Ready = nil // Ensure we don't close it twice if the loop restarts
	}

	logger.L().Info("Worker task loop started", zap.String("id", w.ID))

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
					w.Registry.DeleteByWorkflow(ctrl.PurgeData.WorkflowID)
					logger.L().Info("SHM purged for workflow", zap.String("id", ctrl.PurgeData.WorkflowID))

					// Send Acknowledgment
					ack := models.ControlMessage{
						Type: models.ActionPurgeAck,
						PurgeAck: &models.PurgeAck{
							WorkflowID: ctrl.PurgeData.WorkflowID,
							WorkerID:   w.ID,
						},
					}
					ackBody, _ := json.Marshal(ack)
					if err := stream.Send(&flight.FlightData{AppMetadata: ackBody}); err != nil {
						logger.L().Error("Failed to send purge ack", zap.Error(err))
					}
				}
			}
			continue // control messages carry no DataBody
		}

		var task models.StepExecutionTask
		if err := json.Unmarshal(data.DataBody, &task); err != nil {
			logger.L().Error("Failed to unmarshal task", zap.Error(err))
			continue
		}

		logger.L().Info("Received task", zap.String("id", task.TaskID), zap.String("step", task.Step.Call[1]))

		go func(t models.StepExecutionTask) {
			var result models.TaskResult
			var err error

			namespace := t.Step.Call[0]
			if namespace == "__internal" || strings.HasPrefix(namespace, "std/") {
				result, err = w.executeInternalStep(ctx, t)
			} else {
				result, err = w.PluginServer.DispatchTask(ctx, t)
			}

			if err != nil {
				logger.L().Error("Failed to execute task", zap.Error(err))
				// Send failure result back
				result = models.TaskResult{
					TaskID:       t.TaskID,
					Status:       models.TaskStatusFailed,
					ErrorMessage: err.Error(),
				}
			}
			respBody, _ := json.Marshal(result)
			if err := stream.Send(&flight.FlightData{DataBody: respBody}); err != nil {
				logger.L().Error("Failed to send task result", zap.Error(err))
			}
		}(task)
	}
}

func (w *Worker) executeInternalStep(ctx context.Context, task models.StepExecutionTask) (models.TaskResult, error) {
	namespace := task.Step.Call[0]
	stepName := task.Step.Call[1]
	logger.L().Info("Executing internal step", zap.String("namespace", namespace), zap.String("step", stepName), zap.String("task_id", task.TaskID))

	if namespace == "__internal" {
		stepFunc, ok := internal.Registry[stepName]
		if !ok {
			logger.L().Error("Unknown internal step", zap.String("step", stepName), zap.String("task_id", task.TaskID))
			return models.TaskResult{}, fmt.Errorf("unknown internal step: %s", stepName)
		}
		return stepFunc(ctx, task, w.Registry)
	}

	if strings.HasPrefix(namespace, "std/") {
		fullStepName := fmt.Sprintf("%s:%s", namespace, stepName)
		stepFunc, ok := std.Registry[fullStepName]
		if !ok {
			logger.L().Error("Unknown std step", zap.String("step", fullStepName), zap.String("task_id", task.TaskID))
			return models.TaskResult{}, fmt.Errorf("unknown std step: %s", fullStepName)
		}
		return stepFunc(ctx, task, w.Registry)
	}

	return models.TaskResult{}, fmt.Errorf("unsupported internal namespace: %s", namespace)
}

func (w *Worker) UpdateCapabilities(ctx context.Context, capabilities []string, schemas map[string]schema.StepSchemas) error {
	return w.updateCapabilities(ctx, capabilities, schemas, false)
}

func (w *Worker) updateCapabilities(ctx context.Context, capabilities []string, schemas map[string]schema.StepSchemas, isInternal bool) error {
	errCh := make(chan error, 1)
	w.updateCapabilitiesCh <- func(mctx context.Context) {
		// Merge unique capabilities
		capsMap := make(map[string]bool)
		for _, c := range w.Capabilities {
			capsMap[c] = true
		}
		newCaps := false
		for _, c := range capabilities {
			// Protection: Plugins cannot override __internal namespace
			if !isInternal && len(c) >= 11 && c[:11] == "__internal." {
				logger.L().Warn("Plugin attempted to register __internal capability, ignoring", zap.String("capability", c))
				continue
			}

			if !capsMap[c] {
				w.Capabilities = append(w.Capabilities, c)
				capsMap[c] = true
				newCaps = true
			}
		}

		if !newCaps && len(schemas) == 0 {
			errCh <- nil
			return
		}

		// Update schemas
		if w.Schemas == nil {
			w.Schemas = make(map[string]schema.StepSchemas)
		}
		for k, v := range schemas {
			w.Schemas[k] = v
		}

		// Notify Control Plane
		update := models.WorkerCapabilitiesUpdate{
			Capabilities: w.Capabilities,
			Schemas:      w.Schemas,
		}
		body, _ := json.Marshal(update)
		res, err := w.ControlPlane.DoAction(mctx, &flight.Action{
			Type: models.ActionUpdateCapabilities,
			Body: body,
		})
		if err != nil {
			errCh <- fmt.Errorf("failed to update capabilities: %w", err)
			return
		}
		if _, err := res.Recv(); err != nil {
			errCh <- fmt.Errorf("failed to receive update result: %w", err)
			return
		}

		logger.L().Info("Worker capabilities updated", zap.Strings("capabilities", w.Capabilities))
		errCh <- nil
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func NewWorker(cpAddr string, socketPath string) (*Worker, error) {
	conn, err := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to control plane: %w", err)
	}

	return &Worker{
		ID:                   "worker-" + uuid.New().String()[:8],
		ControlPlane:         flight.NewClientFromConn(conn, nil),
		SocketPath:           socketPath,
		updateCapabilitiesCh: make(chan func(context.Context), 100),
		Ready:                make(chan struct{}),
		Registry:             locality.NewDataLocalityRegistry(),
	}, nil
}
