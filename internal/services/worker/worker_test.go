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
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type mockControlPlane struct {
	flight.BaseFlightServer
	RegisteredWorkers chan models.WorkerRegistration
	RegisteredIDs     chan string
	Heartbeats        chan models.WorkerHeartbeat
	HeartbeatIDs      chan string
	Capabilities      chan models.WorkerCapabilitiesUpdate
	DoExchangeFunc    func(stream flight.FlightService_DoExchangeServer) error
}

func (m *mockControlPlane) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	ctx := stream.Context()
	md, _ := metadata.FromIncomingContext(ctx)
	workerID := ""
	if ids := md.Get("worker-id"); len(ids) > 0 {
		workerID = ids[0]
	}

	switch action.Type {
	case models.ActionRegisterWorker:
		var reg models.WorkerRegistration
		json.Unmarshal(action.Body, &reg)
		m.RegisteredWorkers <- reg
		m.RegisteredIDs <- workerID
		return stream.Send(&flight.Result{Body: []byte("OK")})
	case models.ActionHeartbeat:
		var hb models.WorkerHeartbeat
		json.Unmarshal(action.Body, &hb)
		m.Heartbeats <- hb
		m.HeartbeatIDs <- workerID
		return stream.Send(&flight.Result{Body: []byte("OK")})
	case models.ActionUpdateCapabilities:
		var update models.WorkerCapabilitiesUpdate
		json.Unmarshal(action.Body, &update)
		m.Capabilities <- update
		return stream.Send(&flight.Result{Body: []byte("OK")})
	}
	return nil
}

func (m *mockControlPlane) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	if m.DoExchangeFunc != nil {
		return m.DoExchangeFunc(stream)
	}
	return nil
}

func TestWorker_RegistrationAndHeartbeat(t *testing.T) {
	mock := &mockControlPlane{
		RegisteredWorkers: make(chan models.WorkerRegistration, 1),
		RegisteredIDs:     make(chan string, 1),
		Heartbeats:        make(chan models.WorkerHeartbeat, 10),
		HeartbeatIDs:      make(chan string, 10),
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

	w, err := NewWorker(lis.Addr().String(), "/tmp/heddle-worker-test.sock")
	require.NoError(t, err)

	// Clean up socket
	socketPath := "/tmp/heddle-worker-test.sock"
	os.Remove(socketPath)
	w.PluginServer = NewPluginServer(socketPath)

	// Override Start logic to avoid blocking forever in startTaskLoop for this test
	// We'll just test registration and heartbeat

	go func() {
		w.Start(ctx)
	}()

	// Verify registration
	select {
	case id := <-mock.RegisteredIDs:
		assert.Equal(t, w.ID, id)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for registration")
	}

	// Verify heartbeat
	select {
	case id := <-mock.HeartbeatIDs:
		assert.Equal(t, w.ID, id)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for heartbeat")
	}
}

func TestWorker_PluginServer(t *testing.T) {
	socketPath := "/tmp/heddle-worker-plugin-test.sock"
	os.Remove(socketPath)

	ps := NewPluginServer(socketPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go ps.Start(ctx)
	<-ps.Ready

	// Connect as a plugin
	conn, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)

	// Register plugin
	reg := plugin.PluginRegistration{
		Namespace: "test-ns",
		Language:  "go",
	}
	body, _ := json.Marshal(reg)
	_, err = client.DoAction(ctx, &flight.Action{
		Type: plugin.ActionRegisterPlugin,
		Body: body,
	})
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Verify in server
	_, ok := ps.Plugins.Load("test-ns")
	assert.True(t, ok)
}

func TestWorker_CapabilityUpdate(t *testing.T) {
	mock := &mockControlPlane{
		RegisteredWorkers: make(chan models.WorkerRegistration, 1),
		RegisteredIDs:     make(chan string, 1),
		Capabilities:      make(chan models.WorkerCapabilitiesUpdate, 1),
	}

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, mock)
	go srv.Serve(lis)
	defer srv.Stop()

	ctx := t.Context()

	socketPath := "/tmp/heddle-worker-cap-test.sock"
	os.Remove(socketPath)

	w, err := NewWorker(lis.Addr().String(), socketPath)
	require.NoError(t, err)

	go w.Start(ctx)

	// Wait for worker to register
	select {
	case <-mock.RegisteredIDs:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for registration")
	}

	// Wait for worker to be fully ready (plugin server started)
	select {
	case <-w.Ready:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for worker ready")
	}

	// Connect as a plugin
	conn, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)

	// Consume initial internal capabilities update
	select {
	case update := <-mock.Capabilities:
		assert.Contains(t, update.Capabilities, "__internal.identity")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for initial internal capabilities update")
	}

	// Register plugin with capabilities
	reg := plugin.PluginRegistration{
		Namespace:    "test-ns",
		Language:     "go",
		Capabilities: []string{"test-ns.step1", "test-ns.step2"},
	}
	body, _ := json.Marshal(reg)
	_, err = client.DoAction(ctx, &flight.Action{
		Type: plugin.ActionRegisterPlugin,
		Body: body,
	})
	assert.NoError(t, err)

	// Verify capability update reached control plane
	select {
	case update := <-mock.Capabilities:
		assert.Contains(t, update.Capabilities, "test-ns.step1")
		assert.Contains(t, update.Capabilities, "test-ns.step2")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for capability update")
	}
}
