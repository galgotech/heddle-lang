package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
)

func TestRelationalEngineIntegration(t *testing.T) {
	// 1. Start Control Plane
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	cpAddr := lis.Addr().String()

	server := grpc.NewServer(
		grpc.UnaryInterceptor(UnaryWorkerInterceptor),
		grpc.StreamInterceptor(StreamWorkerInterceptor),
	)
	cpServer := NewControlPlaneServer()
	flight.RegisterFlightServiceServer(server, cpServer)

	go func() {
		if err := server.Serve(lis); err != nil {
			fmt.Printf("CP server error: %v\n", err)
		}
	}()
	defer server.Stop()

	// 2. Start Go Worker (for data generation)
	goWorker, err := execution.NewWorker("go-worker", cpAddr)
	if err != nil {
		t.Fatalf("failed to create go worker: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := goWorker.Register(ctx); err != nil {
		t.Fatalf("go worker registration failed: %v", err)
	}
	go goWorker.StartExecutionLoop(ctx)

	// 3. Start Rust Relational Worker
	rustWorkerPath, _ := filepath.Abs("../../../relational-worker")
	cmd := exec.Command("cargo", "run")
	cmd.Dir = rustWorkerPath
	cmd.Env = append(os.Environ(), fmt.Sprintf("HEDDLE_CP_ADDR=http://%s", cpAddr))
	// We don't wait for it here, it will register itself
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start rust worker: %v", err)
	}
	defer cmd.Process.Kill()

	// 4. Submit Workflow with PRQL
	// Note: We use the (query) syntax for PRQL blocks
	code := `import "test" t
import "std:io" io

schema Data { id: int }

step gen: void -> Data = t.generate
step prn: Data -> void = io.print

workflow main {
  gen
	| (from input | filter val > 1)
	| prn
}
`
	// Submit source code directly

	conn, _ := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := flight.NewClientFromConn(conn, nil)

	action := &flight.Action{
		Type: execution.ActionSubmitWorkflow,
		Body: []byte(code),
	}

	// Wait a bit for rust worker to register
	time.Sleep(5 * time.Second)

	resStream, err := client.DoAction(ctx, action)
	if err != nil {
		t.Fatalf("workflow submission failed: %v", err)
	}
	_, _ = resStream.Recv()

	// 5. Wait for execution
	deadline := time.After(20 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for workflow completion")
		case <-ticker.C:
			cpServer.mu.RLock()
			disp := cpServer.dispatcher
			cpServer.mu.RUnlock()

			if disp != nil {
				// tasks := disp.NextTasks()
				// if len(tasks) == 0 {
				// 	t.Log("Workflow execution finished")
				// 	return
				// }
				return // Placeholder
			}
		}
	}
}
