package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
	pb "github.com/galgotech/heddle-lang/sdk/go/proto"
	"github.com/galgotech/heddle-lang/services/control-plane/pkg/manager"
	"github.com/galgotech/heddle-lang/services/control-plane/pkg/scheduler"
	"github.com/galgotech/heddle-lang/services/control-plane/pkg/state"
)

// ControlPlaneServer implements the "Smart Control Plane" (the Brain) in the Heddle architecture.
// It manages the global DAG topology, orchestrates task dispatch via a locality-aware
// scheduler, and maintains the authoritative state of all distributed workers.
type ControlPlaneServer struct {
	flight.BaseFlightServer

	mu       sync.RWMutex
	registry manager.WorkerRegistry
	queue    *scheduler.WorkQueue
	sm       state.StateMachine
	// dispatcher coordinates the concurrent execution of the DAG.
	dispatcher manager.Dispatcher
	program    *ir.ProgramIR

	// workerStreams maps workerID to a dedicated channel for pushing outgoing tasks.
	workerStreams map[string]chan *execution.Task
	// taskResults facilitates synchronization between the asynchronous DoExchange
	// stream and the synchronous executor calls.
	taskResults map[string]chan error

	// locality tracks the physical placement of data across the cluster to optimize
	// for zero-copy memory access via UDS where possible.
	locality manager.DataLocalityRegistry
	// outputs maps node identifiers to their respective memory handles (e.g., Plasma/UDS paths).
	outputs map[string]string // nodeID -> outputHandle
}

// NewControlPlaneServer initializes a new instance of the control plane with
// injected registries and state tracking mechanisms.
func NewControlPlaneServer(
	registry manager.WorkerRegistry,
	queue *scheduler.WorkQueue,
	sm state.StateMachine,
	locality manager.DataLocalityRegistry,
) *ControlPlaneServer {
	s := &ControlPlaneServer{
		registry:      registry,
		queue:         queue,
		sm:            sm,
		workerStreams: make(map[string]chan *execution.Task),
		taskResults:   make(map[string]chan error),
		locality:      locality,
		outputs:       make(map[string]string),
	}
	return s
}

// DoAction handles unary control-plane operations including worker registration,
// heartbeats, and workflow submission.
func (s *ControlPlaneServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	workerID := GetWorkerID(stream.Context())

	switch action.Type {
	case execution.ActionRegisterWorker:
		var reg execution.WorkerRegistration
		if err := json.Unmarshal(action.Body, &reg); err != nil {
			return fmt.Errorf("failed to unmarshal registration: %w", err)
		}

		// Resolve Worker ID from metadata or explicit body registration.
		id := workerID
		if id == "" {
			id = reg.WorkerID
		}

		s.registry.Register(id, reg.Address, reg.UDSAddress, reg.Tags)

		logger.L().Info("Worker registered",
			zap.String("workerID", id),
			zap.String("runtime", reg.Runtime),
			zap.String("address", reg.Address),
			zap.String("uds_address", reg.UDSAddress))
		return stream.Send(&flight.Result{Body: []byte("OK")})

	case execution.ActionHeartbeat:
		var hb execution.Heartbeat
		if err := json.Unmarshal(action.Body, &hb); err != nil {
			return fmt.Errorf("failed to unmarshal heartbeat: %w", err)
		}

		id := workerID
		if id == "" {
			id = hb.WorkerID
		}

		if err := s.registry.Heartbeat(id); err != nil {
			logger.L().Warn("Heartbeat received from unknown worker", zap.String("workerID", id))
		} else {
			logger.L().Info("Heartbeat received",
				zap.String("workerID", id),
				zap.String("status", string(hb.Status)),
				zap.Float64("load", hb.Load))
		}

		return stream.Send(&flight.Result{Body: []byte("OK")})

	case execution.ActionSubmitWorkflow:
		// workflow submission triggers Just-In-Time (JIT) compilation of Heddle source
		// into an Intermediate Representation (IR) and initializes the execution DAG.
		logger.L().Info("Received workflow submission", zap.Int("bytes", len(action.Body)))

		source := string(action.Body)
		c := compiler.New()
		program, err := c.Compile(source)
		if err != nil {
			return fmt.Errorf("failed to compile workflow: %w", err)
		}

		if err := program.Inflate(); err != nil {
			return fmt.Errorf("failed to inflate program: %w", err)
		}

		s.mu.Lock()
		s.program = program
		// Flush state machine and work queue for the new execution context.
		s.sm = state.NewStateMachine()
		s.queue = scheduler.NewWorkQueue(rate.Limit(100), 10, nil)

		// DAG Traversal: Identify leaf nodes (no dependencies) and queue them for execution.
		for id, inst := range program.Instructions {
			if _, ok := inst.(*ir.StepInstruction); ok {
				s.sm.AddNode(state.NewNode(id))
				s.queue.Add(id, 3)
			}
		}

		// Initialize dispatcher if not already active.
		if s.dispatcher == nil {
			// Inject GoroutinePool as the default concurrency implementation.
			pool := manager.NewGoroutinePool()
			s.dispatcher = manager.NewDispatcher(s.queue, s.registry, s.sm, s.executor, pool)
			d := s.dispatcher
			s.mu.Unlock()
			d.Start(5)
		} else {

			s.mu.Unlock()
		}

		logger.L().Info("Workflow initialized", zap.Int("workflows", len(program.Workflows)))
		return stream.Send(&flight.Result{Body: []byte("Workflow initialized successfully")})

	case execution.ActionGetHistory:
		// Retrieves a snapshot of the current state machine for visualization or debugging.
		history := s.sm.GetHistory()
		body, err := json.Marshal(history)
		if err != nil {
			return fmt.Errorf("failed to marshal history: %w", err)
		}
		return stream.Send(&flight.Result{Body: body})

	default:
		return fmt.Errorf("unknown action: %s", action.Type)
	}
}

// executor is the core dispatch logic that maps an IR node to a physical worker.
// It performs locality-aware routing by analyzing predecessor outputs to determine
// if the target worker can access data via LOCAL shared-memory (UDS) or REMOTE gRPC.
func (s *ControlPlaneServer) executor(ctx context.Context, workerID string, nodeID string) error {
	s.mu.RLock()
	ch, ok := s.workerStreams[workerID]
	program := s.program
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("worker stream not found: %s", workerID)
	}

	// Resolve the IR instruction for the target node.
	raw, ok := program.Instructions[nodeID]
	if !ok {
		return fmt.Errorf("node not found: %s", nodeID)
	}
	step, ok := raw.(*ir.StepInstruction)
	if !ok {
		return fmt.Errorf("node is not a step: %s", nodeID)
	}

	if step == nil {
		return fmt.Errorf("step not found: %s", nodeID)
	}

	// Initialize result channel to await task completion via DoExchange updates.
	resCh := make(chan error, 1)
	s.mu.Lock()
	s.taskResults[nodeID] = resCh
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.taskResults, nodeID)
		s.mu.Unlock()
	}()

	// Locality-aware ticket generation determines the optimal data transfer route.
	tickets := make(map[string]*pb.FlightTicket)

	// Identify predecessors to resolve input dependencies and their physical locations.
	for pid, inst := range program.Instructions {
		if stepInst, ok := inst.(*ir.StepInstruction); ok {
			if stepInst.Next == nodeID {
				// Predecessor found; check for a valid memory handle.
				s.mu.RLock()
				handle, exists := s.outputs[pid]
				s.mu.RUnlock()

				if exists {
					// Query DataLocalityRegistry to find the producing worker.
					producerID, found := s.locality.GetProducer(handle)
					if found {
						ticket := &pb.FlightTicket{
							ResourceId: handle,
						}

						if producerID == workerID {
							// LOCAL: Co-located worker can access via Unix Domain Socket (Shared Memory).
							ticket.RouteType = pb.RouteType_LOCAL
							worker, err := s.registry.GetWorker(producerID)
							if err == nil {
								ticket.Address = worker.UDSAddress
							}
						} else {
							// REMOTE: Data must be fetched via Arrow Flight RPC over TCP.
							ticket.RouteType = pb.RouteType_REMOTE
							worker, err := s.registry.GetWorker(producerID)
							if err == nil {
								ticket.Address = worker.Address
							}
						}
						tickets[pid] = ticket
					}
				}
			}
		}
	}

	// Dispatch task through the registered worker stream.
	task := &execution.Task{
		ID:      nodeID,
		Step:    step,
		Tickets: tickets,
	}

	select {
	case ch <- task:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Block until the worker reports success or failure via the TaskUpdate flow.
	select {
	case err := <-resCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DoExchange establishes a persistent, bidirectional Flight stream for task delegation
// and execution telemetry. It manages the lifecycle of worker-specific task channels.
func (s *ControlPlaneServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	workerID := GetWorkerID(stream.Context())
	if workerID == "" {
		return fmt.Errorf("unidentified worker connecting to DoExchange")
	}

	logger.L().Info("Worker established exchange stream", zap.String("workerID", workerID))

	// Initialize the outbound task channel and register it in the stream map.
	ch := make(chan *execution.Task, 10)
	s.mu.Lock()
	s.workerStreams[workerID] = ch
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.workerStreams, workerID)
		s.mu.Unlock()
		close(ch)
	}()

	errCh := make(chan error, 2)

	// Task Dispatch Loop: Serializes and pushes IR tasks to the connected worker.
	go func() {
		for {
			select {
			case <-stream.Context().Done():
				return
			case task, ok := <-ch:
				if !ok {
					return
				}
				body, _ := json.Marshal(task)
				if err := stream.Send(&flight.FlightData{DataBody: body}); err != nil {
					errCh <- fmt.Errorf("failed to send task: %w", err)
					return
				}
			}
		}
	}()

	// Telemetry Feedback Loop: Processes TaskUpdates (success/failure) from the worker.
	go func() {
		for {
			data, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}

			var update execution.TaskUpdate
			if err := json.Unmarshal(data.DataBody, &update); err == nil {
				logger.L().Info("Received TaskUpdate",
					zap.String("taskID", update.TaskID),
					zap.String("status", string(update.Status)))

				s.mu.RLock()
				resCh, ok := s.taskResults[update.TaskID]
				s.mu.RUnlock()

				if ok {
					var taskErr error
					if update.Status == "completed" {
						// Update output tracking and locality registry for subsequent DAG steps.
						s.mu.Lock()
						s.outputs[update.TaskID] = update.OutputHandle
						s.mu.Unlock()
						s.locality.RegisterOutput(update.OutputHandle, workerID)
					} else if update.Status == "failed" {
						taskErr = fmt.Errorf("%s", update.Error)
					}
					// Signal the blocking executor call that the task has finished.
					resCh <- taskErr
				}
			}
		}
	}()

	err := <-errCh
	logger.L().Info("Exchange stream closed", zap.String("workerID", workerID), zap.Error(err))
	return nil
}

// StartServer bootstraps the gRPC and Flight service listener for the Control Plane.
func StartServer(port int) {
	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.L().Fatal("failed to listen", zap.Error(err))
	}

	// Configure gRPC server with interceptors for worker identification.
	server := grpc.NewServer(
		grpc.UnaryInterceptor(UnaryWorkerInterceptor),
		grpc.StreamInterceptor(StreamWorkerInterceptor),
	)

	registry := manager.NewRegistry()
	queue := scheduler.NewWorkQueue(rate.Limit(100), 10, nil)
	sm := state.NewStateMachine()
	locality := manager.NewDataLocalityRegistry()

	cpServer := NewControlPlaneServer(registry, queue, sm, locality)
	flight.RegisterFlightServiceServer(server, cpServer)

	logger.L().Info("Control Plane Flight Server listening", zap.String("address", addr))
	if err := server.Serve(lis); err != nil {
		logger.L().Fatal("failed to serve", zap.Error(err))
	}
}
