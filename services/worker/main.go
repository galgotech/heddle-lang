package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/galgotech/heddle-lang/pkg/execution"
	_ "github.com/galgotech/heddle-lang/pkg/stdlib/io"
)

func main() {
	workerID := flag.String("id", "worker-1", "Unique ID for this worker")
	cpAddr := flag.String("cp", "localhost:50051", "Address of the control plane")
	flag.Parse()

	worker, err := execution.NewWorker(*workerID, *cpAddr)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	if err := worker.Register(ctx); err != nil {
		log.Fatalf("Failed to register worker: %v", err)
	}

	go worker.StartHeartbeat(ctx)
	go worker.StartExecutionLoop(ctx)

	log.Printf("Worker %s is running", *workerID)
	<-ctx.Done()
	log.Printf("Worker %s shutting down", *workerID)
}
