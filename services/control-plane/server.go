package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"

	"github.com/galgotech/heddle-lang/pkg/execution"
	"github.com/galgotech/heddle-lang/pkg/ir"
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

		log.Printf("Worker registered: %s (%s) at %s", reg.WorkerID, reg.Runtime, reg.Address)
		return stream.Send(&flight.Result{Body: []byte("OK")})

	case execution.ActionHeartbeat:
		var hb execution.Heartbeat
		if err := json.Unmarshal(action.Body, &hb); err != nil {
			return fmt.Errorf("failed to unmarshal heartbeat: %w", err)
		}

		s.mu.Lock()
		if _, ok := s.workers[hb.WorkerID]; ok {
			s.heartbeats[hb.WorkerID] = hb
			log.Printf("Heartbeat received from %s (Status: %s, Load: %.2f)", hb.WorkerID, hb.Status, hb.Load)
		} else {
			log.Printf("Heartbeat received from unknown worker: %s", hb.WorkerID)
		}
		s.mu.Unlock()

		return stream.Send(&flight.Result{Body: []byte("OK")})

	case execution.ActionSubmitWorkflow:
		log.Printf("Received workflow submission (%d bytes)", len(action.Body))
		
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

		log.Printf("Workflow initialized with %d entry points", len(program.Workflows))
		return stream.Send(&flight.Result{Body: []byte("Workflow initialized successfully")})

	default:
		return fmt.Errorf("unknown action: %s", action.Type)
	}
}

func (s *ControlPlaneServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	log.Printf("Worker established exchange stream")

	// This is a simplified execution loop. 
	// In a real implementation, we'd have a central loop that monitors dispatcher and idle workers.
	for {
		s.mu.RLock()
		disp := s.dispatcher
		s.mu.RUnlock()

		if disp != nil {
			tasks := disp.NextTasks()
			for _, task := range tasks {
				log.Printf("Dispatching task %s (%s) to worker", task.ID, task.Step.DefinitionName)
				
				body, _ := json.Marshal(task)
				if err := stream.Send(&flight.FlightData{DataBody: body}); err != nil {
					return fmt.Errorf("failed to send task: %w", err)
				}
			}
		}

		data, err := stream.Recv()
		if err != nil {
			log.Printf("Exchange stream closed: %v", err)
			return nil
		}

		var update execution.TaskUpdate
		if err := json.Unmarshal(data.DataBody, &update); err == nil {
			log.Printf("Received TaskUpdate: %s -> %s", update.TaskID, update.Status)
			s.mu.Lock()
			if s.dispatcher != nil {
				s.dispatcher.ReportUpdate(update)
			}
			s.mu.Unlock()
		} else {
			log.Printf("Received non-update data from worker")
		}
		
		time.Sleep(1 * time.Second) // Slow down the loop for now
	}
}

func StartServer(port int) {
	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	cpServer := NewControlPlaneServer()
	flight.RegisterFlightServiceServer(server, cpServer)

	log.Printf("Control Plane Flight Server listening on %s", addr)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
