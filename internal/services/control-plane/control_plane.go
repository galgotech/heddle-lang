package controlplane

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
	"github.com/galgotech/heddle-lang/sdk/go/proto"
)

const maxTaskRetries = 3

// Internal request types for the ControlPlaneServer manager goroutine.
type registerStreamRequest struct {
	workerID string
	streamCh chan *execution.Task
}

type unregisterStreamRequest struct {
	workerID string
}

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

// ControlPlane implements the "Smart Control Plane" (the Brain) in the Heddle architecture.
// It uses a lock-free design with a central manager goroutine.
type ControlPlane struct {
	workerRegistry WorkerRegistry
	workQueue      *WorkQueue
	dataLocality   DataLocalityRegistry

	// Channels for the manager goroutine
	registerStreamCh   chan registerStreamRequest
	unregisterStreamCh chan unregisterStreamRequest
	getStreamCh        chan getStreamRequest
	registerResultCh   chan registerResultRequest
	unregisterResultCh chan unregisterResultRequest
	taskUpdateCh       chan execution.TaskUpdate
	registerOutputCh   chan registerOutputRequest

	ctx    context.Context
	cancel context.CancelFunc
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

// RegisterStream adds a task stream for a worker.
func (s *ControlPlane) RegisterStream(workerID string, streamCh chan *execution.Task) {
	s.registerStreamCh <- registerStreamRequest{workerID, streamCh}
}

// UnregisterStream removes a task stream for a worker.
func (s *ControlPlane) UnregisterStream(workerID string) {
	s.unregisterStreamCh <- unregisterStreamRequest{workerID}
}

// ReportTaskUpdate processes a status update from a task.
func (s *ControlPlane) ReportTaskUpdate(update execution.TaskUpdate, workerID string) {
	logger.L().Info("Received TaskUpdate",
		zap.String("taskID", update.TaskID),
		zap.String("status", string(update.Status)))

	if update.Status == string(execution.TaskStatusDone) {
		s.registerOutputCh <- registerOutputRequest{update.TaskID, update.OutputHandle}
		s.dataLocality.RegisterOutput(update.OutputHandle, workerID)
	}
	s.taskUpdateCh <- update
}

// GetTaskStream retrieves the task stream for a specific worker.
func (s *ControlPlane) GetTaskStream(workerID string) (chan *execution.Task, bool) {
	respCh := make(chan chan *execution.Task, 1)
	s.getStreamCh <- getStreamRequest{workerID, respCh}
	ch := <-respCh
	return ch, ch != nil
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
	taskResults := make(map[string]chan execution.TaskUpdate)
	outputs := make(map[string]string)

	for {
		select {
		case req := <-s.registerStreamCh:
			workerStreams[req.workerID] = req.streamCh
		case req := <-s.unregisterStreamCh:
			if ch, ok := workerStreams[req.workerID]; ok {
				close(ch)
				delete(workerStreams, req.workerID)
			}
		case req := <-s.getStreamCh:
			req.respCh <- workerStreams[req.workerID]
		case req := <-s.registerResultCh:
			taskResults[req.taskID] = req.respCh
		case req := <-s.unregisterResultCh:
			delete(taskResults, req.taskID)
		case update := <-s.taskUpdateCh:
			if ch, ok := taskResults[update.TaskID]; ok {
				ch <- update
			}
		case req := <-s.registerOutputCh:
			outputs[req.taskID] = req.outputHandle
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
	var err error

	if len(task.Step.Call) >= 2 {
		workers, _ := s.workerRegistry.GetWorkersByCapability(task.Step.Call[0], task.Step.Call[1])
		if len(workers) > 0 {
			worker = workers[0]
		}
	}

	if worker == nil {
		worker, err = s.workerRegistry.GetHealthyWorker()
		if err != nil {
			return execution.TaskUpdate{}, err
		}
	}

	resCh := make(chan execution.TaskUpdate, 1)
	s.registerResultCh <- registerResultRequest{task.ID, resCh}

	defer func() {
		s.unregisterResultCh <- unregisterResultRequest{task.ID}
	}()

	// Inject tickets for inputs from previous steps
	for _, ticket := range task.Tickets {
		if producerID, found := s.dataLocality.GetProducer(ticket.ResourceId); found {
			if producerID == worker.ID {
				ticket.RouteType = proto.RouteType_LOCAL
			} else {
				ticket.RouteType = proto.RouteType_REMOTE
				w, _ := s.workerRegistry.GetWorker(producerID)
				if w != nil {
					ticket.Address = w.Address
				}
			}
		}
	}

	getRespCh := make(chan chan *execution.Task, 1)
	s.getStreamCh <- getStreamRequest{worker.ID, getRespCh}
	stream := <-getRespCh

	if stream == nil {
		return execution.TaskUpdate{}, fmt.Errorf("worker %s disconnected", worker.ID)
	}

	select {
	case stream <- task:
	case <-ctx.Done():
		return execution.TaskUpdate{}, ctx.Err()
	}

	select {
	case update := <-resCh:
		if update.Status == string(execution.TaskStatusFailed) {
			return update, fmt.Errorf("%s", update.Error)
		}
		return update, nil
	case <-ctx.Done():
		return execution.TaskUpdate{}, ctx.Err()
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
		registerStreamCh:   make(chan registerStreamRequest),
		unregisterStreamCh: make(chan unregisterStreamRequest),
		getStreamCh:        make(chan getStreamRequest),
		registerResultCh:   make(chan registerResultRequest),
		unregisterResultCh: make(chan unregisterResultRequest),
		taskUpdateCh:       make(chan execution.TaskUpdate),
		registerOutputCh:   make(chan registerOutputRequest),
		ctx:                ctx,
		cancel:             cancel,
	}
	return s
}
