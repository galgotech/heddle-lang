package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/pkg/data"
)

type Worker struct {
	ID     string
	CPAddr string
	Client flight.Client
	conn   *grpc.ClientConn

	dataMgr *data.DataManager

	// Plugin server
	flight.BaseFlightServer
	pluginAddr string
}

func NewWorker(id, cpAddr string) (*Worker, error) {
	conn, err := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CP: %w", err)
	}

	client := flight.NewClientFromConn(conn, nil)

	return &Worker{
		ID:         id,
		CPAddr:     cpAddr,
		Client:     client,
		conn:       conn,
		dataMgr:    data.NewDataManager("/dev/shm/heddle", 1<<30), // 1GB limit
		pluginAddr: "localhost:50052",                             // Default plugin server address
	}, nil
}

func (w *Worker) Register(ctx context.Context) error {
	reg := WorkerRegistration{
		WorkerID: w.ID,
		Address:  "localhost:0", // In a real scenario, this would be the worker's listen address
		Runtime:  "go",
	}

	body, _ := json.Marshal(reg)
	action := &flight.Action{
		Type: ActionRegisterWorker,
		Body: body,
	}

	stream, err := w.Client.DoAction(ctx, action)
	if err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}

	_, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive registration result: %w", err)
	}

	log.Printf("Worker %s registered successfully", w.ID)
	return nil
}

func (w *Worker) StartHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hb := Heartbeat{
				WorkerID:  w.ID,
				Timestamp: time.Now(),
				Status:    WorkerStatusIdle,
			}
			body, _ := json.Marshal(hb)
			action := &flight.Action{
				Type: ActionHeartbeat,
				Body: body,
			}

			stream, err := w.Client.DoAction(ctx, action)
			if err != nil {
				log.Printf("Heartbeat failed: %v", err)
				continue
			}
			_, _ = stream.Recv() // Drain result
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) StartExecutionLoop(ctx context.Context) {
	stream, err := w.Client.DoExchange(ctx)
	if err != nil {
		log.Fatalf("failed to open exchange stream: %v", err)
	}

	log.Printf("Worker %s execution loop started", w.ID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			data, err := stream.Recv()
			if err != nil {
				log.Printf("Execution stream closed: %v", err)
				return
			}

			var task Task
			if err := json.Unmarshal(data.DataBody, &task); err != nil {
				log.Printf("Failed to unmarshal task: %v", err)
				continue
			}

			log.Printf("Executing task %s (%s)", task.ID, task.Step.DefinitionName)

			// Execute step
			outputHandle, err := w.executeTask(ctx, task)

			// Report update
			update := TaskUpdate{
				TaskID:       task.ID,
				Status:       string(TaskStatusDone),
				OutputHandle: outputHandle,
				Timestamp:    time.Now(),
			}
			if err != nil {
				update.Status = string(TaskStatusFailed)
				update.Error = err.Error()
			}

			updateBody, _ := json.Marshal(update)
			if err := stream.Send(&flight.FlightData{DataBody: updateBody}); err != nil {
				log.Printf("Failed to send task update: %v", err)
			}
		}
	}
}

func (w *Worker) executeTask(ctx context.Context, task Task) (string, error) {
	module := task.Step.Call[0]
	name := task.Step.Call[1]

	fn, ok := GlobalRegistry.Get(module, name)
	if !ok {
		return "", fmt.Errorf("step implementation not found: %s:%s", module, name)
	}

	var input arrow.Record
	var err error

	if task.InputHandle != "" {
		input, err = w.dataMgr.Get(task.InputHandle)
		if err != nil {
			return "", fmt.Errorf("failed to get input from shm: %w", err)
		}
		defer input.Release()
	}

	output, err := fn(ctx, input)
	if err != nil {
		return "", err
	}

	if output != nil {
		defer output.Release()
		handle := fmt.Sprintf("shm-%s-%d", task.ID, time.Now().UnixNano())
		if err := w.dataMgr.Put(handle, output); err != nil {
			return "", fmt.Errorf("failed to put output to shm: %w", err)
		}
		return handle, nil
	}

	return "", nil
}

// DoExchange implements the plugin server's exchange logic.
func (w *Worker) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	log.Println("New plugin client connected via DoExchange")
	for {
		_, err := stream.Recv()
		if err != nil {
			return err
		}
		// Here we would send tasks to the plugin
	}
}
