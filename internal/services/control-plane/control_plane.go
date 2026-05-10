package control_plane

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"net"
	"os"
	"strings"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

type ControlPlaneServer struct {
	flight.BaseFlightServer
	Registry        *WorkerRegistry
	Queue           *TaskQueue
	ActiveStreams   sync.Map // map[string]flight.FlightService_DoExchangeServer
	pendingResults  sync.Map // map[string]chan models.TaskResult
	workflowResults sync.Map // map[string]error
	workflowWaiters sync.Map // map[string]chan error
	mu              sync.Mutex
	Ready           chan struct{}
}

func (s *ControlPlaneServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	ctx := stream.Context()
	metaData, _ := metadata.FromIncomingContext(ctx)

	workerID := ""
	if ids := metaData.Get("worker-id"); len(ids) > 0 {
		workerID = ids[0]
	}

	// Actions that require worker-id
	if action.Type != models.ActionSubmitWorkflow {
		if workerID == "" {
			return status.Error(codes.Unauthenticated, "missing worker-id")
		}
	}

	switch action.Type {
	case models.ActionRegisterWorker:
		var reg models.WorkerRegistration
		if err := json.Unmarshal(action.Body, &reg); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal registration: %v", err)
		}

		s.Registry.Register(workerID, reg)
		logger.L().Info("Worker registered", zap.String("id", workerID), zap.String("address", reg.Address))
		return stream.Send(&flight.Result{Body: []byte("OK")})

	case models.ActionHeartbeat:
		var hb models.WorkerHeartbeat
		if err := json.Unmarshal(action.Body, &hb); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal heartbeat: %v", err)
		}
		if ok := s.Registry.Heartbeat(workerID, hb.Load); !ok {
			return status.Errorf(codes.NotFound, "worker %s not registered", workerID)
		}
		return stream.Send(&flight.Result{Body: []byte("OK")})

	case models.ActionUpdateCapabilities:
		var update models.WorkerCapabilitiesUpdate
		if err := json.Unmarshal(action.Body, &update); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal capabilities update: %v", err)
		}
		if ok := s.Registry.UpdateCapabilities(workerID, update.Capabilities); !ok {
			return status.Errorf(codes.NotFound, "worker %s not registered", workerID)
		}
		logger.L().Info("Worker capabilities updated", zap.String("id", workerID), zap.Strings("capabilities", update.Capabilities))
		return stream.Send(&flight.Result{Body: []byte("OK")})

	case models.ActionSubmitWorkflow:
		var sub models.WorkflowSubmission
		if err := json.Unmarshal(action.Body, &sub); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal submission: %v", err)
		}

		logger.L().Info("Received workflow submission")

		// 1. Compile
		c := compiler.New()
		program, err := c.Compile(sub.Source)
		if err != nil {
			return status.Errorf(codes.Internal, "compilation failed: %v", err)
		}

		// 2. Queue
		task := models.Task{
			ID:      uuid.New().String(),
			Program: program,
		}
		s.Queue.Push(task)

		logger.L().Info("Workflow compiled and queued", zap.String("task_id", task.ID))
		return stream.Send(&flight.Result{Body: fmt.Appendf(nil, "QUEUED: %s", task.ID)})

	default:
		return status.Errorf(codes.Unimplemented, "action %s not implemented", action.Type)
	}
}

func (s *ControlPlaneServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	ctx := stream.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get("worker-id")) == 0 {
		return status.Error(codes.Unauthenticated, "missing worker-id")
	}
	workerID := md.Get("worker-id")[0]

	s.ActiveStreams.Store(workerID, stream)
	defer s.ActiveStreams.Delete(workerID)

	logger.L().Info("Worker connected", zap.String("id", workerID))

	// Goroutine to receive results from this worker and route them to the orchestrator
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			var result models.TaskResult
			if err := json.Unmarshal(resp.DataBody, &result); err != nil {
				logger.L().Warn("Failed to unmarshal result", zap.Error(err))
				continue
			}

			if chVal, ok := s.pendingResults.Load(result.TaskID); ok {
				ch := chVal.(chan models.TaskResult)
				ch <- result
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			task := s.Queue.Pop()
			logger.L().Info("Worker orchestrating task", zap.String("id", workerID), zap.String("task_id", task.ID))

			go s.orchestrateTask(ctx, task)
		}
	}
}

func (s *ControlPlaneServer) orchestrateTask(ctx context.Context, task models.Task) {
	program := task.Program
	for _, flowID := range program.Workflows {
		flow := program.Instructions[flowID].(*ir.FlowInstruction)
		for _, headID := range flow.Heads {
			if err := s.executeStepRecursive(ctx, task.ID, program, headID); err != nil {
				logger.L().Error("Task failed", zap.Error(err))
				s.signalWorkflow(task.ID, err)
				return
			}
		}
	}
	logger.L().Info("Task completed successfully", zap.String("id", task.ID))
	s.signalWorkflow(task.ID, nil)
}

func (s *ControlPlaneServer) signalWorkflow(id string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if chVal, ok := s.workflowWaiters.Load(id); ok {
		ch := chVal.(chan error)
		ch <- err
		s.workflowWaiters.Delete(id)
	} else {
		s.workflowResults.Store(id, err)
	}
}

func (s *ControlPlaneServer) WaitForWorkflow(taskID string) chan error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch := make(chan error, 1)
	if errVal, ok := s.workflowResults.Load(taskID); ok {
		if errVal == nil {
			ch <- nil
		} else {
			ch <- errVal.(error)
		}
		s.workflowResults.Delete(taskID)
		return ch
	}

	s.workflowWaiters.Store(taskID, ch)
	return ch
}

func (s *ControlPlaneServer) executeStepRecursive(ctx context.Context, workflowID string, prog *ir.Program, stepID string) error {
	step := prog.Instructions[stepID].(*ir.StepInstruction)
	capability := fmt.Sprintf("%s.%s", step.Call[0], step.Call[1])

	// 1. Find worker
	worker := s.Registry.FindWorkerForStep(capability)
	if worker == nil {
		return fmt.Errorf("no worker found for capability: %s", capability)
	}

	// 2. Get worker stream
	streamVal, ok := s.ActiveStreams.Load(worker.ID)
	if !ok {
		return fmt.Errorf("worker %s stream not found", worker.ID)
	}
	workerStream := streamVal.(flight.FlightService_DoExchangeServer)

	// 3. Create result channel and register it
	resultCh := make(chan models.TaskResult, 1)
	s.pendingResults.Store(stepID, resultCh)
	defer s.pendingResults.Delete(stepID)

	// 4. Dispatch step
	execTask := models.StepExecutionTask{
		WorkflowID: workflowID,
		TaskID:     stepID,
		Step:       step,
	}
	body, err := json.Marshal(execTask)
	if err != nil {
		return fmt.Errorf("failed to marshal step: %w", err)
	}
	if err := workerStream.Send(&flight.FlightData{DataBody: body}); err != nil {
		return fmt.Errorf("failed to send step to worker %s: %w", worker.ID, err)
	}

	// 5. Wait for result
	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-resultCh:
		if res.Status != models.TaskStatusSuccess {
			return fmt.Errorf("step %s failed: %s", stepID, res.ErrorMessage)
		}
	case <-time.After(30 * time.Second):
		return fmt.Errorf("step %s timed out", stepID)
	}

	// 6. Continue to next steps
	for _, nextID := range step.Next {
		if err := s.executeStepRecursive(ctx, workflowID, prog, nextID); err != nil {
			return err
		}
	}

	return nil
}

func (s *ControlPlaneServer) Listen(addr string) error {
	var lis net.Listener
	var err error

	if after, ok := strings.CutPrefix(addr, "unix://"); ok {
		path := after
		if _, err := os.Stat(path); err == nil {
			os.Remove(path)
		}
		lis, err = net.Listen("unix", path)
	} else {
		lis, err = net.Listen("tcp", addr)
	}

	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)

	logger.L().Info("Control Plane listening", zap.String("address", addr))

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(lis)
	}()

	if s.Ready != nil {
		close(s.Ready)
		s.Ready = nil
	}

	return <-errCh
}

func NewControlPlaneServer() *ControlPlaneServer {
	return &ControlPlaneServer{
		Registry:        NewWorkerRegistry(),
		Queue:           NewTaskQueue(),
		ActiveStreams:   sync.Map{},
		pendingResults:  sync.Map{},
		workflowResults: sync.Map{},
		workflowWaiters: sync.Map{},
		Ready:           make(chan struct{}),
	}
}
