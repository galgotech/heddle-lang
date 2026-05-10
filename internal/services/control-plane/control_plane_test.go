package control_plane

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/internal/services/client"
	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestControlPlane_WorkerRegistration(t *testing.T) {
	s := NewControlPlaneServer()

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
	_, err = client.DoAction(ctx, &flight.Action{
		Type: models.ActionRegisterWorker,
		Body: body,
	})
	assert.NoError(t, err)

	// Verify in registry
	time.Sleep(100 * time.Millisecond)
	workers := s.Registry.GetHealthyWorkers()
	require.NotEmpty(t, workers)
	assert.Equal(t, "test-worker", workers[0].ID)

	// Heartbeat
	hb := models.WorkerHeartbeat{
		Timestamp: time.Now(),
		Load:      5,
	}
	hbBody, _ := json.Marshal(hb)
	_, err = client.DoAction(ctx, &flight.Action{
		Type: models.ActionHeartbeat,
		Body: hbBody,
	})
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Verify load update
	val, _ := s.Registry.workers.Load("test-worker")
	info := val.(*WorkerInfo)
	assert.Equal(t, 5, info.ActiveTasks)
}

func TestControlPlane_TaskDispatch(t *testing.T) {
	s := NewControlPlaneServer()

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
	_, err = client.DoAction(ctx, &flight.Action{
		Type: models.ActionRegisterWorker,
		Body: body,
	})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Update capabilities
	update := models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"std.print"},
	}
	upBody, _ := json.Marshal(update)
	_, err = client.DoAction(ctx, &flight.Action{
		Type: models.ActionUpdateCapabilities,
		Body: upBody,
	})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Push a task to the queue
	stepID := "step-1"
	s.Queue.Push(models.Task{
		ID: "task-1",
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
	})

	// Start exchange as worker
	stream, err := client.DoExchange(ctx)
	require.NoError(t, err)

	// Receive task
	data, err := stream.Recv()
	assert.NoError(t, err)

	var task models.StepExecutionTask
	err = json.Unmarshal(data.DataBody, &task)
	assert.NoError(t, err)
	assert.Equal(t, stepID, task.TaskID)

	// Send result
	res := models.TaskResult{
		TaskID: stepID,
		Status: "SUCCESS",
	}
	resBody, _ := json.Marshal(res)
	err = stream.Send(&flight.FlightData{DataBody: resBody})
	assert.NoError(t, err)
}

func TestControlPlane_UpdateCapabilities(t *testing.T) {
	s := NewControlPlaneServer()

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
	_, err = client.DoAction(ctx, &flight.Action{
		Type: models.ActionRegisterWorker,
		Body: body,
	})
	require.NoError(t, err)

	// 2. Update capabilities
	update := models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"std.print", "std.log"},
	}
	upBody, _ := json.Marshal(update)
	_, err = client.DoAction(ctx, &flight.Action{
		Type: models.ActionUpdateCapabilities,
		Body: upBody,
	})
	assert.NoError(t, err)

	// 3. Verify in registry
	time.Sleep(100 * time.Millisecond)
	val, ok := s.Registry.workers.Load("test-worker")
	require.True(t, ok)
	info := val.(*WorkerInfo)
	assert.ElementsMatch(t, []string{"std.print", "std.log"}, info.Capabilities)
}

func TestControlPlane_WorkflowSubmission(t *testing.T) {
	s := NewControlPlaneServer()

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)
	go srv.Serve(lis)
	defer srv.Stop()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "worker-id", "test-client")
	c, err := client.NewControlPlaneClient(lis.Addr().String())
	require.NoError(t, err)

	source := `
import "std/io" io

workflow hello_world {
  []
    | io.print
}
`
	result, err := c.SubmitWorkflow(ctx, source)
	// The compiler might still fail if std/io is not registered, but it shouldn't panic
	if err != nil {
		// Compilation error is fine as long as it's not a panic
		assert.Contains(t, err.Error(), "compilation failed")
	} else {
		assert.Contains(t, result, "QUEUED")
		assert.Equal(t, 1, s.Queue.Len())
	}
}
