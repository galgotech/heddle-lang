package lsp

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

// mockFlightRegistryServer implements flight.FlightServiceServer to test GetRegistry.
type mockFlightRegistryServer struct {
	flight.BaseFlightServer
	registry *models.RegistryInfo
}

func (s *mockFlightRegistryServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if action.Type == models.ActionGetRegistry {
		body, err := json.Marshal(s.registry)
		if err != nil {
			return err
		}
		return stream.Send(&flight.Result{Body: body})
	}
	return fmt.Errorf("unsupported action: %s", action.Type)
}

func TestControlPlaneLSPClient_InitialState(t *testing.T) {
	client := NewControlPlaneLSPClient("localhost:9999")
	assert.False(t, client.IsConnected(), "New client should not be connected initially")
}

func TestControlPlaneLSPClient_ConnectAndClose(t *testing.T) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, &mockFlightRegistryServer{})
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	client := NewControlPlaneLSPClient(lis.Addr().String())
	assert.False(t, client.IsConnected())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	assert.NoError(t, err)
	assert.True(t, client.IsConnected())

	// Redundant connect should be safe and return nil
	err = client.Connect(ctx)
	assert.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
	assert.False(t, client.IsConnected())

	// Redundant close should be safe and return nil
	err = client.Close()
	assert.NoError(t, err)
}

func TestControlPlaneLSPClient_GetRegistry(t *testing.T) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	expectedRegistry := &models.RegistryInfo{
		Steps: map[string]schema.StepSchemas{
			"postgres.query": {
				Documentation: "Execute query",
			},
		},
	}

	mockServer := &mockFlightRegistryServer{
		registry: expectedRegistry,
	}

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, mockServer)
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	client := NewControlPlaneLSPClient(lis.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Calling GetRegistry before connecting should return an error
	_, err = client.GetRegistry(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not connected to control plane")

	err = client.Connect(ctx)
	require.NoError(t, err)
	defer client.Close()

	registry, err := client.GetRegistry(ctx)
	assert.NoError(t, err)
	require.NotNil(t, registry)
	assert.Contains(t, registry.Steps, "postgres.query")
	assert.Equal(t, "Execute query", registry.Steps["postgres.query"].Documentation)
}
