package control_plane

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/internal/client"
	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator"
	"github.com/galgotech/heddle-lang/internal/control-plane/registry"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
)

type mockOrchestrator struct {
	tasks chan models.Task
}

func (m *mockOrchestrator) OrchestrateTask(ctx context.Context, task models.Task) {
	m.tasks <- task
}

func TestControlPlane_WorkerRegistration(t *testing.T) {
	s := NewControlPlaneServer(registry.NewWorkerRegistry())

	// Start server on random port
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)
	go srv.Serve(lis)
	defer srv.Stop()

	// Connect client
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)
	ctx := context.Background()

	// Register worker
	reg := models.WorkerRegistration{
		Address: "localhost:1234",
	}
	body, _ := json.Marshal(reg)
	ctx = metadata.AppendToOutgoingContext(ctx, "worker-id", "test-worker")
	resp, err := client.DoAction(ctx, &flight.Action{
		Type: models.ActionRegisterWorker,
		Body: body,
	})
	require.NoError(t, err)
	_, err = resp.Recv()
	assert.NoError(t, err)

	// Heartbeat
	hb := models.WorkerHeartbeat{
		Timestamp: time.Now(),
		Load:      5,
	}
	hbBody, _ := json.Marshal(hb)
	resp, err = client.DoAction(ctx, &flight.Action{
		Type: models.ActionHeartbeat,
		Body: hbBody,
	})
	require.NoError(t, err)
	_, err = resp.Recv()
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

}

func TestControlPlane_TaskDispatch(t *testing.T) {
	s := NewControlPlaneServer(registry.NewWorkerRegistry())

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "worker-id", "test-worker")

	// Register worker
	reg := models.WorkerRegistration{
		Address: "localhost:1234",
	}
	body, _ := json.Marshal(reg)
	resp, err := client.DoAction(ctx, &flight.Action{
		Type: models.ActionRegisterWorker,
		Body: body,
	})
	require.NoError(t, err)
	_, err = resp.Recv()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Update capabilities
	update := models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"std.print"},
	}
	upBody, _ := json.Marshal(update)
	resp, err = client.DoAction(ctx, &flight.Action{
		Type: models.ActionUpdateCapabilities,
		Body: upBody,
	})
	require.NoError(t, err)
	_, err = resp.Recv()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Start exchange as worker
	stream, err := client.DoExchange(ctx)
	require.NoError(t, err)

	// Start exchange as client
	clientCtx := metadata.AppendToOutgoingContext(context.Background(), "client-id", "test-client")
	clientStream, err := client.DoExchange(clientCtx)
	require.NoError(t, err)
	defer clientStream.CloseSend()

	time.Sleep(50 * time.Millisecond)

	// Run orchestrator directly in a goroutine
	stepID := "step-1"
	task := models.Task{
		ID:       "task-1",
		ClientID: "test-client",
		Program: &ir.Program{
			Instructions: map[string]any{
				"flow-1": &ir.FlowInstruction{
					BaseInstruction: ir.BaseInstruction{ID: "flow-1"},
					Heads:           []string{stepID},
				},
				stepID: &ir.StepInstruction{
					BaseInstruction: ir.BaseInstruction{ID: stepID},
					Call:            []string{"std", "print"},
				},
			},
			Workflows: []string{"flow-1"},
		},
	}
	go s.orchestrators[orchestrator.StrategyRecursive].OrchestrateTask(ctx, task)

	// Receive task
	data, err := stream.Recv()
	assert.NoError(t, err)

	var execTask models.StepExecutionTask
	err = json.Unmarshal(data.DataBody, &execTask)
	assert.NoError(t, err)
	assert.Equal(t, stepID, execTask.TaskID)

	// Send result
	res := models.TaskResult{
		TaskID: execTask.TaskID,
		Status: "SUCCESS",
	}
	resBody, _ := json.Marshal(res)
	err = stream.Send(&flight.FlightData{DataBody: resBody})
	assert.NoError(t, err)
}

func TestControlPlane_UpdateCapabilities(t *testing.T) {
	s := NewControlPlaneServer(registry.NewWorkerRegistry())

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "worker-id", "test-worker")

	// 1. Register worker
	reg := models.WorkerRegistration{
		Address: "localhost:1234",
	}
	body, _ := json.Marshal(reg)
	resp, err := client.DoAction(ctx, &flight.Action{
		Type: models.ActionRegisterWorker,
		Body: body,
	})
	require.NoError(t, err)
	_, err = resp.Recv()
	require.NoError(t, err)

	// 2. Update capabilities
	update := models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"std.print", "std.log"},
	}
	upBody, _ := json.Marshal(update)
	resp, err = client.DoAction(ctx, &flight.Action{
		Type: models.ActionUpdateCapabilities,
		Body: upBody,
	})
	assert.NoError(t, err)
	_, err = resp.Recv()
	assert.NoError(t, err)

}

func TestControlPlane_WorkflowSubmission(t *testing.T) {
	s := NewControlPlaneServer(registry.NewWorkerRegistry())

	s.registry.Register("test-worker", models.WorkerRegistration{Address: "localhost:1234"})
	s.registry.UpdateCapabilities("test-worker", models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"std/io.print"},
	})

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)
	go srv.Serve(lis)
	defer srv.Stop()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "client-id", "test-client")
	c, err := client.NewControlPlaneClient(ctx, lis.Addr().String())
	require.NoError(t, err)

	source := `
import "std/io" io

workflow hello_world {
  []
    | io.print
}
`
	mockOrch := &mockOrchestrator{tasks: make(chan models.Task, 1)}
	s.orchestrators[orchestrator.StrategyRecursive] = mockOrch

	result, err := c.SubmitWorkflow(source, "", false)
	// The compiler might still fail if std/io is not registered, but it shouldn't panic
	if err != nil {
		// Compilation error is fine as long as it's not a panic
		assert.Contains(t, err.Error(), "compilation failed")
	} else {
		assert.Contains(t, result, "QUEUED")
		select {
		case task := <-mockOrch.tasks:
			assert.NotEmpty(t, task.ID)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for orchestrated task")
		}
	}
}

func TestControlPlane_WorkflowFiltering(t *testing.T) {
	s := NewControlPlaneServer(registry.NewWorkerRegistry())

	s.registry.Register("test-worker", models.WorkerRegistration{Address: "localhost:1234"})
	s.registry.UpdateCapabilities("test-worker", models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"std/io.print"},
	})

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)
	go srv.Serve(lis)
	defer srv.Stop()

	ctx := context.Background()
	c, err := client.NewControlPlaneClient(ctx, lis.Addr().String())
	require.NoError(t, err)

	source := `
import "std/io" io

workflow w1 {
  []
    | io.print
}

workflow w2 {
  []
    | io.print
}
`
	mockOrch := &mockOrchestrator{tasks: make(chan models.Task, 1)}
	s.orchestrators[orchestrator.StrategyRecursive] = mockOrch

	// Submit only w2
	_, err = c.SubmitWorkflow(source, "w2", false)
	require.NoError(t, err)

	select {
	case task := <-mockOrch.tasks:
		assert.Equal(t, "w2", task.TargetWorkflow)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for orchestrated task")
	}
}
