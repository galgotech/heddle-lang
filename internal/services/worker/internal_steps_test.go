package worker

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

func TestWorker_InternalSteps(t *testing.T) {
	mock := &mockControlPlane{
		RegisteredWorkers: make(chan models.WorkerRegistration, 1),
		RegisteredIDs:     make(chan string, 1),
		Capabilities:      make(chan models.WorkerCapabilitiesUpdate, 10),
	}

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, mock)
	go srv.Serve(lis)
	defer srv.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	socketPath := "/tmp/heddle-worker-internal-test.sock"
	os.Remove(socketPath)

	w, err := NewWorker(lis.Addr().String(), socketPath)
	require.NoError(t, err)

	// We need to capture the exchange stream to send a task
	exchangeCh := make(chan flight.FlightService_DoExchangeServer, 1)
	mock.DoExchangeFunc = func(stream flight.FlightService_DoExchangeServer) error {
		exchangeCh <- stream
		<-stream.Context().Done()
		return nil
	}

	go w.Start(ctx)

	// Wait for worker to register and start exchange
	var stream flight.FlightService_DoExchangeServer
	select {
	case stream = <-exchangeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for exchange stream")
	}

	// Send an internal task
	task := models.StepExecutionTask{
		TaskID: "task-1",
		Step: &ir.StepInstruction{
			Call: []string{"__internal", "identity"},
		},
	}
	body, _ := json.Marshal(task)
	err = stream.Send(&flight.FlightData{DataBody: body})
	require.NoError(t, err)

	// Verify result
	resCh := make(chan *flight.FlightData, 1)
	errCh := make(chan error, 1)
	go func() {
		res, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		resCh <- res
	}()

	select {
	case res := <-resCh:
		var result models.TaskResult
		err = json.Unmarshal(res.DataBody, &result)
		require.NoError(t, err)
		assert.Equal(t, "task-1", result.TaskID)
		assert.Equal(t, models.TaskStatusSuccess, result.Status)
	case err := <-errCh:
		t.Fatalf("Error receiving from stream: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for task result")
	}
}

func TestWorker_ProtectInternalNamespace(t *testing.T) {
	mock := &mockControlPlane{
		RegisteredWorkers: make(chan models.WorkerRegistration, 1),
		RegisteredIDs:     make(chan string, 1),
		Capabilities:      make(chan models.WorkerCapabilitiesUpdate, 10),
	}

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, mock)
	go srv.Serve(lis)
	defer srv.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	socketPath := "/tmp/heddle-worker-protect-test.sock"
	os.Remove(socketPath)

	w, err := NewWorker(lis.Addr().String(), socketPath)
	require.NoError(t, err)

	go w.Start(ctx)

	// Wait for initial registration
	select {
	case update := <-mock.Capabilities:
		assert.Contains(t, update.Capabilities, "__internal.identity")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for initial capabilities")
	}

	// Connect as a plugin and try to register an internal capability
	conn, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)

	reg := plugin.PluginRegistration{
		Namespace:    "evil-plugin",
		Language:     "go",
		Capabilities: []string{"__internal.identity", "normal-step"},
	}
	body, _ := json.Marshal(reg)
	_, err = client.DoAction(ctx, &flight.Action{
		Type: plugin.ActionRegisterPlugin,
		Body: body,
	})
	require.NoError(t, err)

	// Verify that normal-step was added but __internal.identity was ignored (it already exists, but we want to ensure it wasn't processed from the plugin)
	// Actually, if it already exists, the merge logic will skip it anyway.
	// But our protection logic should warn and skip it before even checking the map.

	select {
	case update := <-mock.Capabilities:
		assert.Contains(t, update.Capabilities, "normal-step")
		// The count should be 4: identity, prql, data_literal + normal-step
		assert.Len(t, update.Capabilities, 4)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for capability update")
	}
}
