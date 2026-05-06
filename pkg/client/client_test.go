package heddlesdk

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
)

type mockControlPlane struct {
	flight.BaseFlightServer
}

func (s *mockControlPlane) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if action.Type == execution.ActionSubmitWorkflow {
		return stream.Send(&flight.Result{Body: []byte("Workflow received successfully")})
	}
	return stream.Send(&flight.Result{Body: []byte("Unknown action")})
}

func TestControlPlaneClient_SubmitWorkflow(t *testing.T) {
	// Start mock server
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()

	s := grpc.NewServer()
	flight.RegisterFlightServiceServer(s, &mockControlPlane{})

	go func() {
		if err := s.Serve(lis); err != nil {
			return
		}
	}()
	defer s.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create client
	client, err := NewControlPlaneClient(addr)
	assert.NoError(t, err)
	defer client.Close()

	// Test submission with valid syntax
	workflow := []byte("workflow main {\n    step1\n}")
	result, err := client.SubmitWorkflow(context.Background(), workflow)
	assert.NoError(t, err)
	assert.Equal(t, "Workflow received successfully", result)
}

func TestControlPlaneClient_SubmitWorkflow_InvalidSyntax(t *testing.T) {
	// Create client (no server needed for local syntax check)
	client := &ControlPlaneClient{}

	// Test submission with invalid syntax
	workflow := []byte("invalid_keyword hello {\n \n}")
	result, err := client.SubmitWorkflow(context.Background(), workflow)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workflow submission aborted due to")
	assert.Empty(t, result)
}

func TestControlPlaneClient_ConnectError(t *testing.T) {
	// Try to connect to a non-existent server
	_, err := NewControlPlaneClient("localhost:12345")
	assert.NoError(t, err)
}
