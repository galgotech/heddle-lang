package server

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
	pb "github.com/galgotech/heddle-lang/sdk/go/proto"
	"github.com/galgotech/heddle-lang/services/control-plane/pkg/manager"
	"github.com/galgotech/heddle-lang/services/control-plane/pkg/scheduler"
	"github.com/galgotech/heddle-lang/services/control-plane/pkg/state"
)

func TestControlPlane_LocalityAwareTickets(t *testing.T) {
	// 1. Start Control Plane
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	cpAddr := lis.Addr().String()

	server := grpc.NewServer(
		grpc.UnaryInterceptor(UnaryWorkerInterceptor),
		grpc.StreamInterceptor(StreamWorkerInterceptor),
	)
	
	registry := manager.NewRegistry()
	queue := scheduler.NewWorkQueue(rate.Limit(100), 10, nil)
	sm := state.NewStateMachine()
	locality := manager.NewDataLocalityRegistry()

	cpServer := NewControlPlaneServer(registry, queue, sm, locality)
	flight.RegisterFlightServiceServer(server, cpServer)

	go func() {
		_ = server.Serve(lis)
	}()
	defer server.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := flight.NewClientFromConn(conn, nil)

	// 2. Register two workers
	// Worker 1 has UDS
	reg1 := execution.WorkerRegistration{
		WorkerID:   "worker-1",
		Address:    "10.0.0.1:50051",
		UDSAddress: "/tmp/worker-1.sock",
	}
	body1, _ := json.Marshal(reg1)
	stream1, err := client.DoAction(ctx, &flight.Action{Type: execution.ActionRegisterWorker, Body: body1})
	require.NoError(t, err)
	_, err = stream1.Recv()
	require.NoError(t, err)

	// Worker 2 has no UDS (or different one)
	reg2 := execution.WorkerRegistration{
		WorkerID: "worker-2",
		Address:  "10.0.0.2:50051",
	}
	body2, _ := json.Marshal(reg2)
	stream2, err := client.DoAction(ctx, &flight.Action{Type: execution.ActionRegisterWorker, Body: body2})
	require.NoError(t, err)
	_, err = stream2.Recv()
	require.NoError(t, err)

	// 3. Setup a mock program with two steps: s1 -> s2
	s1 := &ir.StepInstruction{
		BaseInstruction: ir.BaseInstruction{ID: "s1", Type: ir.StepInst},
		Next:            "s2",
	}
	s2 := &ir.StepInstruction{
		BaseInstruction: ir.BaseInstruction{ID: "s2", Type: ir.StepInst},
	}
	program := &ir.ProgramIR{
		Instructions: map[string]interface{}{
			"s1": s1,
			"s2": s2,
		},
	}
	cpServer.program = program

	// 4. Simulate s1 completion on worker-1
	outputHandle := "handle-1"
	
	// Simulate receiving this update
	cpServer.mu.Lock()
	cpServer.outputs["s1"] = outputHandle
	cpServer.mu.Unlock()
	cpServer.locality.RegisterOutput(outputHandle, "worker-1")

	// 5. Test LOCAL ticket: dispatch s2 to worker-1
	ch1 := make(chan *execution.Task, 1)
	cpServer.mu.Lock()
	cpServer.workerStreams["worker-1"] = ch1
	cpServer.mu.Unlock()

	go func() {
		_ = cpServer.executor(ctx, "worker-1", "s2")
	}()

	select {
	case task := <-ch1:
		ticket, ok := task.Tickets["s1"]
		require.True(t, ok, "task should have ticket for s1")
		assert.Equal(t, pb.RouteType_LOCAL, ticket.RouteType)
		assert.Equal(t, "/tmp/worker-1.sock", ticket.Address)
		assert.Equal(t, outputHandle, ticket.ResourceId)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for task on worker-1")
	}

	// 6. Test REMOTE ticket: dispatch s2 to worker-2
	ch2 := make(chan *execution.Task, 1)
	cpServer.mu.Lock()
	cpServer.workerStreams["worker-2"] = ch2
	cpServer.mu.Unlock()

	go func() {
		_ = cpServer.executor(ctx, "worker-2", "s2")
	}()

	select {
	case task := <-ch2:
		ticket, ok := task.Tickets["s1"]
		require.True(t, ok, "task should have ticket for s1")
		assert.Equal(t, pb.RouteType_REMOTE, ticket.RouteType)
		assert.Equal(t, "10.0.0.1:50051", ticket.Address)
		assert.Equal(t, outputHandle, ticket.ResourceId)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for task on worker-2")
	}
}
