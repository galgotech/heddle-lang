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
)

type ControlPlaneServer struct {
	flight.BaseFlightServer

	mu      sync.RWMutex
	workers map[string]*execution.WorkerRegistration
	health  map[string]time.Time
}

func NewControlPlaneServer() *ControlPlaneServer {
	return &ControlPlaneServer{
		workers: make(map[string]*execution.WorkerRegistration),
		health:  make(map[string]time.Time),
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
		s.workers[reg.WorkerID] = &reg
		s.health[reg.WorkerID] = time.Now()
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
			s.health[hb.WorkerID] = time.Now()
			log.Printf("Heartbeat received from %s", hb.WorkerID)
		}
		s.mu.Unlock()

		return stream.Send(&flight.Result{Body: []byte("OK")})

	default:
		return fmt.Errorf("unknown action: %s", action.Type)
	}
}

func (s *ControlPlaneServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	// For this initial implementation, we just echo back a "Connection established" message
	// and wait for messages. Real logic would involve sending IR and receiving TaskUpdates.
	log.Printf("Worker established exchange stream")

	for {
		data, err := stream.Recv()
		if err != nil {
			log.Printf("Exchange stream closed: %v", err)
			return nil
		}

		// Process incoming TaskUpdates or results from worker
		log.Printf("Received data from worker via exchange")
		_ = data
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
