package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

type Worker struct {
	ID                   string
	ControlPlane         flight.Client
	SocketPath           string
	Capabilities         []string
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
	if err := w.updateCapabilities(ctx, []string{
		"__internal.identity",
		"__internal.prql",
		"__internal.data_literal",
	}, true); err != nil {
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

		var task models.StepExecutionTask
		if err := json.Unmarshal(data.DataBody, &task); err != nil {
			logger.L().Error("Failed to unmarshal task", zap.Error(err))
			continue
		}

		logger.L().Info("Received task", zap.String("id", task.TaskID), zap.String("step", task.Step.Call[1]))

		go func(t models.StepExecutionTask) {
			var result models.TaskResult
			var err error

			if t.Step.Call[0] == "__internal" {
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
	stepName := task.Step.Call[1]
	logger.L().Info("Executing internal step", zap.String("step", stepName), zap.String("task_id", task.TaskID))

	switch stepName {
	case "identity":
		// TODO: Implement identity step
		return models.TaskResult{
			TaskID: task.TaskID,
			Status: models.TaskStatusSuccess,
		}, nil
	case "prql":
		// TODO: Implement PRQL execution via DataFusion
		return models.TaskResult{
			TaskID: task.TaskID,
			Status: models.TaskStatusSuccess,
		}, nil
	case "data_literal":
		logger.L().Info("Executing data_literal step", zap.String("task_id", task.TaskID))

		data, ok := task.Step.Config["data"]
		if !ok {
			return models.TaskResult{}, fmt.Errorf("data_literal: missing 'data' in config")
		}

		listData, ok := data.([]any)
		if !ok {
			return models.TaskResult{}, fmt.Errorf("data_literal: 'data' must be a list of objects")
		}

		record, err := convertToArrowRecord(listData)
		if err != nil {
			return models.TaskResult{}, fmt.Errorf("data_literal: failed to convert to arrow: %w", err)
		}

		// Store data in the locality registry for zero-copy access by plugins.
		// The task ID serves as the handle for subsequent steps.
		handle := task.TaskID

		f, err := locality.AllocateAndWrite(record)
		if err != nil {
			return models.TaskResult{}, fmt.Errorf("data_literal: failed to write to SHM: %w", err)
		}
		path := f.Name()
		f.Close()
		logger.L().Info("Allocated data_literal to SHM", zap.String("handle", handle), zap.String("path", path))

		// literal_data always is first step the tips is void -> data_type
		isOutputVoid := len(task.Step.OutputType) > 0 && task.Step.OutputType[0] == models.VoidType
		if isOutputVoid {
			logger.L().Warn("data_literal is first step and output is void, this is not expected", zap.String("handle", handle))
		}
		w.Registry.Put(locality.NewMetadata(handle, locality.Output, path))

		return models.TaskResult{
			TaskID: task.TaskID,
			Status: models.TaskStatusSuccess,
		}, nil
	default:
		logger.L().Error("Unknown internal step", zap.String("step", stepName), zap.String("task_id", task.TaskID))
		return models.TaskResult{}, fmt.Errorf("unknown internal step: %s", stepName)
	}
}

func (w *Worker) UpdateCapabilities(ctx context.Context, capabilities []string) error {
	return w.updateCapabilities(ctx, capabilities, false)
}

func (w *Worker) updateCapabilities(ctx context.Context, capabilities []string, isInternal bool) error {
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

		if !newCaps {
			errCh <- nil
			return
		}

		// Notify Control Plane
		update := models.WorkerCapabilitiesUpdate{
			Capabilities: w.Capabilities,
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

func convertToArrowRecord(data []any) (arrow.Record, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
	}

	first, ok := data[0].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid data format: expected list of maps, got %T", data[0])
	}

	// 1. Infer schema from first element
	keys := make([]string, 0, len(first))
	for k := range first {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fields := make([]arrow.Field, 0, len(keys))
	for _, k := range keys {
		v := first[k]
		var dt arrow.DataType
		switch v.(type) {
		case float64:
			dt = arrow.PrimitiveTypes.Float64
		case string:
			dt = arrow.BinaryTypes.String
		case bool:
			dt = arrow.FixedWidthTypes.Boolean
		default:
			dt = arrow.BinaryTypes.String // Fallback
		}
		fields = append(fields, arrow.Field{Name: k, Type: dt})
	}
	schema := arrow.NewSchema(fields, nil)

	// 2. Build columns
	mem := memory.NewGoAllocator()
	builders := make([]array.Builder, len(fields))
	for i, f := range fields {
		builders[i] = array.NewBuilder(mem, f.Type)
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	for _, item := range data {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		for i, f := range fields {
			val := m[f.Name]
			if val == nil {
				builders[i].AppendNull()
				continue
			}

			switch b := builders[i].(type) {
			case *array.Float64Builder:
				if fv, ok := val.(float64); ok {
					b.Append(fv)
				} else {
					b.AppendNull()
				}
			case *array.StringBuilder:
				b.Append(fmt.Sprint(val))
			case *array.BooleanBuilder:
				if bv, ok := val.(bool); ok {
					b.Append(bv)
				} else {
					b.AppendNull()
				}
			default:
				builders[i].AppendNull()
			}
		}
	}

	cols := make([]arrow.Array, len(fields))
	for i := range fields {
		cols[i] = builders[i].NewArray()
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	return array.NewRecord(schema, cols, int64(len(data))), nil
}
