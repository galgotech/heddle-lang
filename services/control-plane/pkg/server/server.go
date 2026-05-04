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

type ControlPlaneServer struct {
	flight.BaseFlightServer

	mu       sync.RWMutex
	registry *manager.Registry
	queue    *scheduler.WorkQueue
	sm       *state.StateMachine
	// Current dispatcher (if any)
	dispatcher *manager.Dispatcher
	program    *ir.ProgramIR

	// workerStreams maps workerID -> channel for outgoing tasks
	workerStreams map[string]chan *execution.Task
	// taskResults maps nodeID -> channel for execution results
	taskResults map[string]chan error

	// Data Locality
	locality *manager.DataLocalityRegistry
	outputs  map[string]string // nodeID -> outputHandle
}

func NewControlPlaneServer() *ControlPlaneServer {
	s := &ControlPlaneServer{
		registry:      manager.NewRegistry(),
		queue:         scheduler.NewWorkQueue(rate.Limit(100), 10, nil),
		sm:            state.NewStateMachine(),
		workerStreams: make(map[string]chan *execution.Task),
		taskResults:   make(map[string]chan error),
		locality:      manager.NewDataLocalityRegistry(),
		outputs:       make(map[string]string),
	}
	return s
}

func (s *ControlPlaneServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	workerID := GetWorkerID(stream.Context())

	switch action.Type {
	case execution.ActionRegisterWorker:
		var reg execution.WorkerRegistration
		if err := json.Unmarshal(action.Body, &reg); err != nil {
			return fmt.Errorf("failed to unmarshal registration: %w", err)
		}

		// Use the ID from metadata if available, otherwise from body
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
		// Reset state machine and queue for new workflow
		s.sm = state.NewStateMachine()
		s.queue = scheduler.NewWorkQueue(rate.Limit(100), 10, nil)

		// Populate queue with initial nodes (those with no dependencies)
		// This is a placeholder for actual DAG traversal
		for id, inst := range program.Instructions {
			if _, ok := inst.(*ir.StepInstruction); ok {
				s.sm.AddNode(state.NewNode(id))
				s.queue.Add(id, 3)
			}
		}

		if s.dispatcher == nil {
			s.dispatcher = manager.NewDispatcher(s.queue, s.registry, s.executor)
			s.dispatcher.Start(5)
		}
		s.mu.Unlock()

		logger.L().Info("Workflow initialized", zap.Int("workflows", len(program.Workflows)))
		return stream.Send(&flight.Result{Body: []byte("Workflow initialized successfully")})

	case execution.ActionGetHistory:
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

func (s *ControlPlaneServer) executor(ctx context.Context, workerID string, nodeID string) error {
	s.mu.RLock()
	ch, ok := s.workerStreams[workerID]
	program := s.program
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("worker stream not found: %s", workerID)
	}

	// Find the step in the program
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

	// Create result channel
	resCh := make(chan error, 1)
	s.mu.Lock()
	s.taskResults[nodeID] = resCh
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.taskResults, nodeID)
		s.mu.Unlock()
	}()

	// sending task with locality-aware tickets
	tickets := make(map[string]*pb.FlightTicket)

	// 1. Find predecessors to identify inputs
	for pid, inst := range program.Instructions {
		if stepInst, ok := inst.(*ir.StepInstruction); ok {
			if stepInst.Next == nodeID {
				// We found a predecessor
				s.mu.RLock()
				handle, exists := s.outputs[pid]
				s.mu.RUnlock()

				if exists {
					// 2. Query locality registry for the producer
					producerID, found := s.locality.GetProducer(handle)
					if found {
						// 3. Compare with target worker to determine ticket type
						ticket := &pb.FlightTicket{
							ResourceId: handle,
						}

						if producerID == workerID {
							// Local access
							ticket.RouteType = pb.RouteType_LOCAL
							worker, err := s.registry.GetWorker(producerID)
							if err == nil {
								ticket.Address = worker.UDSAddress
							}
						} else {
							// Remote access
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

	// Send task
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

	// Wait for result
	select {
	case err := <-resCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ControlPlaneServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	workerID := GetWorkerID(stream.Context())
	if workerID == "" {
		return fmt.Errorf("unidentified worker connecting to DoExchange")
	}

	logger.L().Info("Worker established exchange stream", zap.String("workerID", workerID))

	// Register the stream channel
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

	// Error group/channel to manage bidirectional flow
	errCh := make(chan error, 2)

	// Goroutine to send tasks to worker
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

	// Goroutine to receive updates from worker
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
						// Record output handle and locality
						s.mu.Lock()
						s.outputs[update.TaskID] = update.OutputHandle
						s.mu.Unlock()
						s.locality.RegisterOutput(update.OutputHandle, workerID)
					} else if update.Status == "failed" {
						taskErr = fmt.Errorf("%s", update.Error)
					}
					resCh <- taskErr
				}
			}
		}
	}()

	// Wait for one of the goroutines to fail or the stream to close
	err := <-errCh
	logger.L().Info("Exchange stream closed", zap.String("workerID", workerID), zap.Error(err))
	return nil
}

func StartServer(port int) {
	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.L().Fatal("failed to listen", zap.Error(err))
	}

	server := grpc.NewServer(
		grpc.UnaryInterceptor(UnaryWorkerInterceptor),
		grpc.StreamInterceptor(StreamWorkerInterceptor),
	)
	cpServer := NewControlPlaneServer()
	flight.RegisterFlightServiceServer(server, cpServer)

	logger.L().Info("Control Plane Flight Server listening", zap.String("address", addr))
	if err := server.Serve(lis); err != nil {
		logger.L().Fatal("failed to serve", zap.Error(err))
	}
}
