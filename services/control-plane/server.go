package main

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/galgotech/heddle-lang/pkg/execution"
	"github.com/galgotech/heddle-lang/pkg/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

type ControlPlaneServer struct {
	flight.BaseFlightServer

	mu         sync.RWMutex
	workers    map[string]execution.WorkerRegistration
	heartbeats map[string]execution.Heartbeat

	// Active dispatcher for the current workflow
	dispatcher *execution.Dispatcher
}

func NewControlPlaneServer() *ControlPlaneServer {
	return &ControlPlaneServer{
		workers:    make(map[string]execution.WorkerRegistration),
		heartbeats: make(map[string]execution.Heartbeat),
	}
}

func (s *ControlPlaneServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch action.Type {
	case execution.ActionRegisterWorker:
		var reg execution.WorkerRegistration
		if err := json.Unmarshal(action.Body, &reg); err != nil {
			return fmt.Errorf("failed to unmarshal registration: %w", err)
		}

		s.mu.Lock()
		s.workers[reg.WorkerID] = reg
		// Initialize heartbeat entry on registration
		s.heartbeats[reg.WorkerID] = execution.Heartbeat{
			WorkerID:  reg.WorkerID,
			Timestamp: time.Now(),
			Status:    execution.WorkerStatusIdle,
		}
		s.mu.Unlock()

		logger.L().Info("Worker registered",
			zap.String("workerID", reg.WorkerID),
			zap.String("runtime", reg.Runtime),
			zap.String("address", reg.Address))
		return stream.Send(&flight.Result{Body: []byte("OK")})

	case execution.ActionHeartbeat:
		var hb execution.Heartbeat
		if err := json.Unmarshal(action.Body, &hb); err != nil {
			return fmt.Errorf("failed to unmarshal heartbeat: %w", err)
		}

		s.mu.Lock()
		if _, ok := s.workers[hb.WorkerID]; ok {
			s.heartbeats[hb.WorkerID] = hb
			logger.L().Info("Heartbeat received",
				zap.String("workerID", hb.WorkerID),
				zap.String("status", string(hb.Status)),
				zap.Float64("load", hb.Load))
		} else {
			logger.L().Warn("Heartbeat received from unknown worker", zap.String("workerID", hb.WorkerID))
		}
		s.mu.Unlock()

		return stream.Send(&flight.Result{Body: []byte("OK")})

	case execution.ActionSubmitWorkflow:
		logger.L().Info("Received workflow submission", zap.Int("bytes", len(action.Body)))

		var program ir.ProgramIR
		if err := json.Unmarshal(action.Body, &program); err != nil {
			return fmt.Errorf("failed to unmarshal IR: %w", err)
		}

		if err := program.Inflate(); err != nil {
			return fmt.Errorf("failed to inflate IR: %w", err)
		}

		s.mu.Lock()
		s.dispatcher = execution.NewDispatcher(&program)
		s.mu.Unlock()

		logger.L().Info("Workflow initialized", zap.Int("entryPoints", len(program.Workflows)))
		return stream.Send(&flight.Result{Body: []byte("Workflow initialized successfully")})

	case execution.ActionGetHistory:
		s.mu.RLock()
		defer s.mu.RUnlock()
		if s.dispatcher == nil {
			return fmt.Errorf("no active workflow")
		}

		body, err := json.Marshal(s.dispatcher.History)
		if err != nil {
			return fmt.Errorf("failed to marshal history: %w", err)
		}
		return stream.Send(&flight.Result{Body: body})

	default:
		return fmt.Errorf("unknown action: %s", action.Type)
	}
}

func (s *ControlPlaneServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	logger.L().Info("Worker established exchange stream")

	// This is a simplified execution loop.
	// In a real implementation, we'd have a central loop that monitors dispatcher and idle workers.
	for {
		s.mu.RLock()
		disp := s.dispatcher
		s.mu.RUnlock()

		if disp != nil {
			tasks := disp.NextTasks()
			for _, task := range tasks {
				logger.L().Info("Dispatching task",
					zap.String("taskID", task.ID),
					zap.String("step", task.Step.DefinitionName))

				body, _ := json.Marshal(task)
				if err := stream.Send(&flight.FlightData{DataBody: body}); err != nil {
					return fmt.Errorf("failed to send task: %w", err)
				}
			}
		}

		data, err := stream.Recv()
		if err != nil {
			logger.L().Info("Exchange stream closed", zap.Error(err))
			return nil
		}

		var update execution.TaskUpdate
		if err := json.Unmarshal(data.DataBody, &update); err == nil {
			logger.L().Info("Received TaskUpdate",
				zap.String("taskID", update.TaskID),
				zap.String("status", string(update.Status)))
			s.mu.Lock()
			if s.dispatcher != nil {
				s.dispatcher.ReportUpdate(update)
			}
			s.mu.Unlock()
		} else {
			logger.L().Warn("Received non-update data from worker")
		}

		time.Sleep(1 * time.Second) // Slow down the loop for now
	}
}

func StartServer(port int) {
	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.L().Fatal("failed to listen", zap.Error(err))
	}

	server := grpc.NewServer()
	cpServer := NewControlPlaneServer()
	flight.RegisterFlightServiceServer(server, cpServer)

	logger.L().Info("Control Plane Flight Server listening", zap.String("address", addr))
	if err := server.Serve(lis); err != nil {
		logger.L().Fatal("failed to serve", zap.Error(err))
	}
}
