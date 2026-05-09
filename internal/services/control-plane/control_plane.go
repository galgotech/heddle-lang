package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
)

const maxTaskRetries = 3

type registerResultRequest struct {
	taskID string
	respCh chan execution.TaskUpdate
}

type unregisterResultRequest struct {
	taskID string
}

type getStreamRequest struct {
	workerID string
	respCh   chan chan *execution.Task
}

type registerOutputRequest struct {
	taskID       string
	outputHandle string
}

type registerStreamRequest struct {
	workerID string
	taskCh   chan *execution.Task
}

// ControlPlane implements the "Smart Control Plane" (the Brain) in the Heddle architecture.
// It uses a lock-free design with a central manager goroutine.
type ControlPlane struct {
	workerRegistry WorkerRegistry
	workQueue      *WorkQueue
	dataLocality   DataLocalityRegistry

	ctx    context.Context
	cancel context.CancelFunc

	// Manager channels
	registerStreamCh   chan registerStreamRequest
	unregisterStreamCh chan string
	getStreamCh        chan getStreamRequest
	registerResultCh   chan registerResultRequest
	unregisterResultCh chan unregisterResultRequest
	reportUpdateCh     chan execution.TaskUpdate
}

// RegisterWorker adds a new worker to the registry.
func (s *ControlPlane) RegisterWorker(reg execution.WorkerRegistration, workerID string) {
	s.workerRegistry.Register(workerID, reg.Address, reg.UDSAddress, reg.Tags)

	logger.L().Info("Worker registered",
		zap.String("workerID", workerID),
		zap.String("runtime", reg.Runtime),
		zap.String("address", reg.Address),
		zap.String("uds_address", reg.UDSAddress))
}

// Heartbeat records a heartbeat from a worker.
func (s *ControlPlane) Heartbeat(hb execution.Heartbeat, workerID string) error {
	if err := s.workerRegistry.Heartbeat(workerID); err != nil {
		logger.L().Warn("Heartbeat received from unknown worker", zap.String("workerID", workerID))
		return err
	}

	logger.L().Info("Heartbeat received",
		zap.String("workerID", workerID),
		zap.String("status", string(hb.Status)),
		zap.Float64("load", hb.Load))
	return nil
}

// SubmitWorkflow compiles and submits a workflow for execution.
func (s *ControlPlane) SubmitWorkflow(source string) error {
	logger.L().Info("Received workflow submission", zap.Int("bytes", len(source)))

	c := compiler.New()
	program, err := c.Compile(source)
	if err != nil {
		return fmt.Errorf("failed to compile workflow: %w", err)
	}

	s.workQueue.Add(program, maxTaskRetries)

	logger.L().Info("Workflow initialized", zap.Int("workflows", len(program.Workflows)))
	return nil
}

// Start begins the background task processing loops, including the manager and worker loops.
func (s *ControlPlane) Start() {
	go s.run()
	go s.workerLoop()
}

// Stop gracefully shuts down the server's background processes.
func (s *ControlPlane) Stop() {
	s.cancel()
	s.workQueue.ShutDown()
}

func (s *ControlPlane) run() {
	workerStreams := make(map[string]chan *execution.Task)
	resultWaiters := make(map[string]chan execution.TaskUpdate)

	for {
		select {
		case req := <-s.registerStreamCh:
			workerStreams[req.workerID] = req.taskCh
		case workerID := <-s.unregisterStreamCh:
			delete(workerStreams, workerID)
		case req := <-s.getStreamCh:
			ch, ok := workerStreams[req.workerID]
			if !ok {
				req.respCh <- nil
			} else {
				req.respCh <- ch
			}
		case req := <-s.registerResultCh:
			resultWaiters[req.taskID] = req.respCh
		case req := <-s.unregisterResultCh:
			delete(resultWaiters, req.taskID)
		case update := <-s.reportUpdateCh:
			if ch, ok := resultWaiters[update.TaskID]; ok {
				ch <- update
				delete(resultWaiters, update.TaskID)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// workerLoop runs the continuous fetch-dispatch-state cycle for task execution.
func (s *ControlPlane) workerLoop() {
	for {
		task, shuttingDown := s.workQueue.Get()
		if shuttingDown {
			return
		}

		err := s.executor(s.ctx, task.Program)
		if err != nil {
			if task.Retries < task.MaxRetries {
				s.workQueue.Retry(task)
			}
		} else {
			s.workQueue.Done(task)
		}
	}
}

// executor orchestrates the DAG execution of a single program.
func (s *ControlPlane) executor(ctx context.Context, program *ir.Program) error {
	dispatcher := execution.NewDispatcher(program)
	updateSignal := make(chan struct{}, 1)

	for {
		tasks := dispatcher.NextTasks()
		if len(tasks) == 0 {
			return nil
		}

		for _, task := range tasks {
			go func(t execution.Task) {
				update, err := s.dispatchTask(ctx, &t)
				if err != nil {
					update = execution.TaskUpdate{
						TaskID:    t.ID,
						Status:    string(execution.TaskStatusFailed),
						Error:     err.Error(),
						Timestamp: time.Now(),
					}
				}
				dispatcher.ReportUpdate(update)
				select {
				case updateSignal <- struct{}{}:
				default:
				}
			}(task)
		}

		select {
		case <-updateSignal:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// dispatchTask selects a worker and transmits a single instruction for execution.
func (s *ControlPlane) dispatchTask(ctx context.Context, task *execution.Task) (execution.TaskUpdate, error) {
	var worker *Worker

	if len(task.Step.Call) >= 2 {
		workers, err := s.workerRegistry.GetWorkersByCapability(task.Step.Call[0], task.Step.Call[1])
		if err != nil {
			return execution.TaskUpdate{}, err
		}
		if len(workers) > 0 {
			worker = workers[0]
		}
	}

	if worker == nil {
		return execution.TaskUpdate{}, fmt.Errorf("no worker found for task %s", task.ID)
	}

	// Retrieve the active task channel for the selected worker.
	respCh := make(chan chan *execution.Task, 1)
	s.getStreamCh <- getStreamRequest{workerID: worker.ID, respCh: respCh}
	taskCh := <-respCh
	if taskCh == nil {
		return execution.TaskUpdate{}, fmt.Errorf("worker %s stream not available", worker.ID)
	}

	// Register a listener for the task's execution result.
	updateCh := make(chan execution.TaskUpdate, 1)
	s.registerResultCh <- registerResultRequest{taskID: task.ID, respCh: updateCh}

	// Transmit the task to the worker's execution stream.
	select {
	case taskCh <- task:
	case <-ctx.Done():
		s.unregisterResultCh <- unregisterResultRequest{taskID: task.ID}
		return execution.TaskUpdate{}, ctx.Err()
	}

	// Block until an update is received or the context expires.
	select {
	case update := <-updateCh:
		return update, nil
	case <-ctx.Done():
		s.unregisterResultCh <- unregisterResultRequest{taskID: task.ID}
		return execution.TaskUpdate{}, ctx.Err()
	}
}

// Exchange establishes a bidirectional communication channel with a worker for task delegation and state updates.
func (s *ControlPlane) Exchange(stream flight.FlightService_DoExchangeServer) error {
	workerID := GetWorkerID(stream.Context())
	if workerID == "" {
		return fmt.Errorf("missing worker identifier in exchange context")
	}

	taskCh := make(chan *execution.Task, 100)
	s.registerStreamCh <- registerStreamRequest{workerID: workerID, taskCh: taskCh}
	defer func() {
		s.unregisterStreamCh <- workerID
	}()

	logger.L().Info("Exchange stream established", zap.String("workerID", workerID))

	// Handle outbound task transmission.
	go func() {
		for {
			select {
			case task := <-taskCh:
				body, err := json.Marshal(task)
				if err != nil {
					logger.L().Error("Failed to marshal task", zap.String("workerID", workerID), zap.Error(err))
					continue
				}
				if err := stream.Send(&flight.FlightData{DataBody: body}); err != nil {
					logger.L().Error("Failed to send task to worker", zap.String("workerID", workerID), zap.Error(err))
					return
				}
			case <-stream.Context().Done():
				return
			}
		}
	}()

	// Handle inbound task updates.
	for {
		data, err := stream.Recv()
		if err != nil {
			logger.L().Info("Exchange stream closed by worker", zap.String("workerID", workerID))
			return nil
		}

		var update execution.TaskUpdate
		if err := json.Unmarshal(data.DataBody, &update); err != nil {
			logger.L().Error("Failed to unmarshal task update", zap.Error(err))
			continue
		}

		s.reportUpdateCh <- update
	}
}

// NewControlPlane initializes a new instance of the control plane.
func NewControlPlane(
	workerRegistry WorkerRegistry,
	workQueue *WorkQueue,
	dataLocality DataLocalityRegistry,
) *ControlPlane {
	ctx, cancel := context.WithCancel(context.Background())
	s := &ControlPlane{
		workerRegistry:     workerRegistry,
		workQueue:          workQueue,
		dataLocality:       dataLocality,
		ctx:                ctx,
		cancel:             cancel,
		registerStreamCh:   make(chan registerStreamRequest),
		unregisterStreamCh: make(chan string),
		getStreamCh:        make(chan getStreamRequest),
		registerResultCh:   make(chan registerResultRequest),
		unregisterResultCh: make(chan unregisterResultRequest),
		reportUpdateCh:     make(chan execution.TaskUpdate),
	}
	return s
}
