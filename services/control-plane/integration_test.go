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

	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/execution"
)

func TestEndToEndDataFlow(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := worker.Register(ctx); err != nil {
		t.Fatalf("worker registration failed: %v", err)
	}

	go worker.StartExecutionLoop(ctx)

	// 3. Submit Workflow with Data Flow
	code := `import "test" t
import "std:io" io

step gen: void -> void = t.generate
step inc: void -> void = t.increment
step prn: void -> void = io.print

workflow main {
  gen
    | inc
    | prn
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

	// 4. Wait for execution and verify results
	deadline := time.After(10 * time.Second)
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
					t.Log("Workflow execution finished")
					return
				}
			}
		}
	}
}
