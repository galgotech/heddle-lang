package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/internal/services/control-plane/manager"
	"github.com/galgotech/heddle-lang/internal/services/control-plane/scheduler"
	"github.com/galgotech/heddle-lang/internal/services/control-plane/state"
	"github.com/galgotech/heddle-lang/pkg/runtime/data"
	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
	"github.com/galgotech/heddle-lang/pkg/runtime/transport"
)

func TestEndToEndDataFlow(t *testing.T) {
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
	registry := manager.NewRegistry()
	queue := scheduler.NewWorkQueue(rate.Inf, 1, nil)
	sm := state.NewStateMachine()
	locality := manager.NewDataLocalityRegistry()

	cpServer := NewControlPlaneServer(registry, queue, sm, locality)
	flight.RegisterFlightServiceServer(server, cpServer)

	go func() {
		if err := server.Serve(lis); err != nil {
			fmt.Printf("CP server error: %v\n", err)
		}
	}()
	defer server.Stop()

	// 2. Start Worker
	workerConn, _ := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	workerTrans := transport.NewFlightTransport(workerConn)
	workerAlloc := data.NewOSMemoryAllocator(t.TempDir())
	workerDataMgr := data.NewLocalMmapManager(workerAlloc, 1<<30)
	worker := execution.NewWorker("worker-1", workerTrans, workerDataMgr, 1, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := worker.Register(ctx); err != nil {
		t.Fatalf("worker registration failed: %v", err)
	}

	go worker.StartExecutionLoop(ctx)

	// 3. Submit Workflow with Data Flow
	code := `
import "test" t
import "std:io" io

step gen = t.generate
step inc = t.increment
step prn = io.print

workflow main {
  gen
    | inc
    | prn
}
`

	conn, _ := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := flight.NewClientFromConn(conn, nil)

	action := &flight.Action{
		Type: execution.ActionSubmitWorkflow,
		Body: []byte(code),
	}

	resStream, err := client.DoAction(ctx, action)
	if err != nil {
		t.Fatalf("workflow submission failed: %v", err)
	}
	res, err := resStream.Recv()
	if err != nil {
		t.Fatalf("workflow submission failed at Recv: %v", err)
	}
	t.Logf("Workflow submission response: %s", string(res.Body))

	// 4. Wait for execution (placeholder)
	time.Sleep(1 * time.Second)
}
