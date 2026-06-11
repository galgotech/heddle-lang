package run

import (
	"bytes"
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/internal/client"
	control_plane "github.com/galgotech/heddle-lang/internal/control-plane"
	"github.com/galgotech/heddle-lang/internal/control-plane/registry"
)

func TestRunCmd_FlagsRegistration(t *testing.T) {
	// Verify that the new execution flags are registered on the command
	asyncFlag := RunCmd.Flags().Lookup("async")
	require.NotNil(t, asyncFlag, "expected --async flag to be registered")
	assert.Equal(t, "bool", asyncFlag.Value.Type())

	interactiveFlag := RunCmd.Flags().Lookup("interactive")
	require.NotNil(t, interactiveFlag, "expected --interactive flag to be registered")
	assert.Equal(t, "bool", interactiveFlag.Value.Type())
}

func TestRunCmd_ExecutionFlows(t *testing.T) {
	// Create a temporary .he file
	tmpFile, err := os.CreateTemp("", "test_workflow_*.he")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(`
workflow hello {
  []
}
`)
	require.NoError(t, err)
	tmpFile.Close()

	// Set up a mock/real Control Plane server on a local random port
	reg := registry.NewWorkerRegistry()
	s := control_plane.NewControlPlaneServer(reg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)
	go srv.Serve(lis)
	defer srv.Stop()

	// 1. Test Asynchronous execution (should submit and release the terminal immediately)
	t.Run("Async execution via --async", func(t *testing.T) {
		t.Setenv("HEDDLE_CLIENT_MODE", "remote")
		t.Setenv("HEDDLE_CLIENT_TARGET", lis.Addr().String())

		RunCmd.SetArgs([]string{
			"--async",
			tmpFile.Name(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err = RunCmd.ExecuteContext(ctx)
		// This should succeed without blocking
		assert.NoError(t, err)
	})

}

func TestSubmitWorkflow_Interactive(t *testing.T) {
	// Start a mock flight server that can handle the interactive loop exchange
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	mockSrv := &mockInteractiveServer{
		receivedData: make(chan string, 2),
	}
	flight.RegisterFlightServiceServer(srv, mockSrv)
	go srv.Serve(lis)
	defer srv.Stop()

	// 1. User approves the step
	t.Run("Interactive Approve", func(t *testing.T) {
		ctx := context.Background()
		c, err := client.NewControlPlaneClient(ctx, lis.Addr().String())
		require.NoError(t, err)

		// Mock the user entering 'y' (yes)
		c.In = bytes.NewBufferString("y\n")

		// Submit workflow in synchronous mode, which enters the read stream loop
		_, err = c.SubmitWorkflow("source", "w", "interactive", false)
		assert.NoError(t, err)

		select {
		case approved := <-mockSrv.receivedData:
			assert.Equal(t, "APPROVE", approved)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for approval from client")
		}
	})

	// 2. User rejects the step
	t.Run("Interactive Reject", func(t *testing.T) {
		ctx := context.Background()
		c, err := client.NewControlPlaneClient(ctx, lis.Addr().String())
		require.NoError(t, err)

		// Mock the user entering 'n' (no)
		c.In = bytes.NewBufferString("n\n")

		_, err = c.SubmitWorkflow("source", "w", "interactive", false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rejected by user")

		select {
		case rejected := <-mockSrv.receivedData:
			assert.Equal(t, "REJECT", rejected)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for rejection from client")
		}
	})
}

type mockInteractiveServer struct {
	flight.BaseFlightServer
	receivedData chan string
}

func (m *mockInteractiveServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Respond to the submit-workflow action immediately
	return stream.Send(&flight.Result{Body: []byte("QUEUED:task-1")})
}

func (m *mockInteractiveServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	// Detect client or worker exchange stream
	metaData, _ := metadata.FromIncomingContext(stream.Context())
	clientIDs := metaData.Get("client-id")
	if len(clientIDs) > 0 {
		// Send prompt
		err := stream.Send(&flight.FlightData{DataBody: []byte("PROMPT:step-1:std.print")})
		if err != nil {
			return err
		}

		// Wait for response
		resp, err := stream.Recv()
		if err != nil {
			return err
		}

		m.receivedData <- string(resp.DataBody)
	}
	return nil
}
