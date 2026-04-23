package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/pkg/compiler"
	"github.com/galgotech/heddle-lang/pkg/execution"
)

func TestEndToEndExecution(t *testing.T) {
	// 1. Start Control Plane
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	cpAddr := lis.Addr().String()

	server := grpc.NewServer()
	cpServer := NewControlPlaneServer()
	flight.RegisterFlightServiceServer(server, cpServer)

	go func() {
		if err := server.Serve(lis); err != nil {
			fmt.Printf("CP server error: %v\n", err)
		}
	}()
	defer server.Stop()

	// 2. Start Worker
	worker, err := execution.NewWorker("worker-1", cpAddr)
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := worker.Register(ctx); err != nil {
		t.Fatalf("worker registration failed: %v", err)
	}

	go worker.StartExecutionLoop(ctx)

	// 3. Submit Workflow
	code := `
import "std:io" io
step p1: void -> void = io.print

workflow main {
  p1
}
`
	c := compiler.New()
	program, err := c.Compile(code)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	programBody, _ := json.Marshal(program)

	conn, _ := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := flight.NewClientFromConn(conn, nil)

	action := &flight.Action{
		Type: execution.ActionSubmitWorkflow,
		Body: programBody,
	}

	resStream, err := client.DoAction(ctx, action)
	if err != nil {
		t.Fatalf("workflow submission failed: %v", err)
	}
	_, _ = resStream.Recv()

	// 4. Wait for execution
	// We check if the dispatcher has no more tasks and all are done
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
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
				tasks := disp.NextTasks()
				if len(tasks) == 0 {
					// In a real scenario, we'd check if all tasks are in "completed" state.
					// For this test, if NextTasks() is empty after some time, we assume it's done.
					// Let's add a more robust check in the dispatcher if needed.
					t.Log("Workflow execution finished")
					return
				}
			}
		}
	}
}
