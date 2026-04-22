package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/pkg/execution"
	_ "github.com/galgotech/heddle-lang/pkg/stdlib/io" // Register stdlib
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Worker struct {
	ID         string
	CPAddr     string
	Client     flight.Client
	conn       *grpc.ClientConn
	
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
		pluginAddr: "localhost:50052", // Default plugin server address
	}, nil
}

func (w *Worker) Register(ctx context.Context) error {
	reg := execution.WorkerRegistration{
		WorkerID: w.ID,
		Address:  "localhost:0", // In a real scenario, this would be the worker's listen address
		Runtime:  "go",
	}

	body, _ := json.Marshal(reg)
	action := &flight.Action{
		Type: execution.ActionRegisterWorker,
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
			hb := execution.Heartbeat{
				WorkerID:  w.ID,
				Timestamp: time.Now(),
				Status:    execution.WorkerStatusIdle,
			}
			body, _ := json.Marshal(hb)
			action := &flight.Action{
				Type: execution.ActionHeartbeat,
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
			// In a real scenario, we would receive IR here and process it.
			// For now, we just keep the stream alive.
			_, err := stream.Recv()
			if err != nil {
				log.Printf("Execution stream closed: %v", err)
				return
			}
		}
	}
}

// StartPluginServer starts the server that plugins connect to.
func (w *Worker) StartPluginServer(ctx context.Context) error {
	lis, err := net.Listen("tcp", w.pluginAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", w.pluginAddr, err)
	}

	server := grpc.NewServer()
	flight.RegisterFlightServiceServer(server, w)

	log.Printf("Worker %s starting plugin server at %s", w.ID, w.pluginAddr)
	
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Printf("Plugin server error: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		server.Stop()
	}()

	return nil
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
