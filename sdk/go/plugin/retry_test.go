package plugin_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type mockFlightServer struct {
	flight.BaseFlightServer
	registered bool
}

func (s *mockFlightServer) DoAction(req *flight.Action, stream flight.FlightService_DoActionServer) error {
	if req.Type == plugin.ActionRegisterPlugin {
		s.registered = true
		return stream.Send(&flight.Result{Body: []byte("ok")})
	}
	return fmt.Errorf("unknown action: %s", req.Type)
}

func (s *mockFlightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	return nil
}

func TestPluginConnectRetry(t *testing.T) {
	socketPath := "/tmp/heddle-worker.sock" // Use the default for now as it's hardcoded
	_ = os.Remove(socketPath)

	p := plugin.New("test-namespace")

	errChan := make(chan error, 1)
	go func() {
		errChan <- p.Start()
	}()

	// Wait a bit to ensure it failed at least once (internally)
	time.Sleep(1 * time.Second)

	// Now start the server
	lis, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	defer lis.Close()
	defer os.Remove(socketPath)

	server := grpc.NewServer()
	mock := &mockFlightServer{}
	flight.RegisterFlightServiceServer(server, mock)

	go server.Serve(lis)
	defer server.Stop()

	// The plugin should now connect and signal readiness
	select {
	case <-p.Ready:
		assert.True(t, mock.registered)
	case <-time.After(10 * time.Second):
		t.Fatal("Plugin failed to signal readiness within timeout")
	}

	// --- TEST RECONNECTION ---

	// Reset mock registration state
	mock.registered = false

	// Stop the server (causing exchange stream error)
	server.Stop()
	lis.Close()

	// Wait a bit for the plugin to detect failure and start retrying
	time.Sleep(2 * time.Second)

	// Restart the server
	lis, err = net.Listen("unix", socketPath)
	require.NoError(t, err)
	defer lis.Close()

	server = grpc.NewServer()
	flight.RegisterFlightServiceServer(server, mock)
	go server.Serve(lis)
	defer server.Stop()

	// The plugin should eventually reconnect and register again
	// We wait for mock.registered to become true (polling is easiest here for mock)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if mock.registered {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.True(t, mock.registered, "Plugin failed to reconnect and re-register")

	// --- STOP PLUGIN VIA SIGNAL ---
	syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	
	select {
	case err := <-errChan:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Plugin failed to exit after signal")
	}
}
