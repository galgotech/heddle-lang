package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Worker struct {
	ID           string
	ControlPlane flight.Client
	Status       string
	SocketPath   string
	Capabilities []string
	PluginServer *PluginServer
}

func (w *Worker) Start(ctx context.Context) error {
	ctx = metadata.AppendToOutgoingContext(ctx, "worker-id", w.ID)

	// 1. Register with Control Plane
	reg := models.WorkerRegistration{
		Address:      "localhost", // Should be actual address
		Capabilities: w.Capabilities,
	}
	body, _ := json.Marshal(reg)
	_, err := w.ControlPlane.DoAction(ctx, &flight.Action{
		Type: models.ActionRegisterWorker,
		Body: body,
	})
	if err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}
	logger.L().Info("Worker registered with control plane", zap.String("id", w.ID))

	// 2. Start Heartbeats
	go w.startHeartbeat(ctx)

	// 3. Start Plugin Server (UDS)
	w.PluginServer = NewPluginServer(w.SocketPath)
	go func() {
		if err := w.PluginServer.Start(ctx); err != nil {
			logger.L().Error("Plugin server error", zap.Error(err))
		}
	}()

	// 4. Start Task Loop
	return w.startTaskLoop(ctx)
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
			result, err := w.PluginServer.DispatchTask(ctx, t)
			if err != nil {
				logger.L().Error("Failed to dispatch task", zap.Error(err))
				return
			}
			respBody, _ := json.Marshal(result)
			if err := stream.Send(&flight.FlightData{DataBody: respBody}); err != nil {
				logger.L().Error("Failed to send task result", zap.Error(err))
			}
		}(task)
	}
}

func (w *Worker) executeTask(ctx context.Context, task models.Task) models.TaskResult {
	// Here we would route the task to the appropriate plugin.
	// For this version, let's just log and return success.
	logger.L().Info("Executing task", zap.String("id", task.ID))

	// Simulate work
	time.Sleep(1 * time.Second)

	return models.TaskResult{
		TaskID: task.ID,
		Status: "SUCCESS",
	}
}

func NewWorker(cpAddr string) (*Worker, error) {
	conn, err := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to control plane: %w", err)
	}

	return &Worker{
		ID:           "worker-" + uuid.New().String()[:8],
		ControlPlane: flight.NewClientFromConn(conn, nil),
		Status:       "starting",
		SocketPath:   "/tmp/heddle-worker.sock",
	}, nil
}
