package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
)

func main() {
	workerID := "worker-" + uuid.New().String()[:8]
	fmt.Printf("Heddle Go Worker %s starting...\n", workerID)

	worker, err := NewWorker(workerID, "localhost:50051")
	if err != nil {
		log.Fatalf("failed to create worker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down worker...")
		cancel()
	}()

	// Start plugin server
	if err := worker.StartPluginServer(ctx); err != nil {
		log.Fatalf("failed to start plugin server: %v", err)
	}

	if err := worker.Register(ctx); err != nil {
		log.Printf("Warning: failed to register worker: %v", err)
		// Don't fatal here in case CP is not up yet, but in production we might want to.
	}

	go worker.StartHeartbeat(ctx)
	worker.StartExecutionLoop(ctx)
}
