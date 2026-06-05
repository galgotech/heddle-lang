package worker

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/pkg/schema"
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

	ctx := t.Context()

	// Clean up socket
	socketPath := "/tmp/heddle-worker-test.sock"
	os.Remove(socketPath)
	registry := locality.NewDataLocalityRegistry()
	nativePlugins := NewNativePlugins()
	ps := NewPluginServer(registry, nativePlugins, socketPath)

	w, err := NewWorker(ps, lis.Addr().String())
	require.NoError(t, err)

	go func() {
		w.Start(ctx)
	}()

	// Verify registration
	select {
	case id := <-mock.RegisteredIDs:
		assert.Equal(t, w.GetID(), id)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for registration")
	}

	// Verify heartbeat
	select {
	case id := <-mock.HeartbeatIDs:
		assert.Equal(t, w.GetID(), id)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for heartbeat")
	}
}

func TestWorker_PluginServer(t *testing.T) {
	socketPath := "/tmp/heddle-worker-plugin-test.sock"
	os.Remove(socketPath)

	registry := locality.NewDataLocalityRegistry()
	nativePlugins := NewNativePlugins()
	ps := NewPluginServer(registry, nativePlugins, socketPath)
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
	ps.pluginsMU.RLock()
	_, ok := ps.plugins["test-ns"]
	ps.pluginsMU.RUnlock()
	assert.True(t, ok)
}

func TestWorker_CapabilityUpdate(t *testing.T) {
	mock := &mockControlPlane{
		RegisteredWorkers: make(chan models.WorkerRegistration, 1),
		RegisteredIDs:     make(chan string, 1),
		Capabilities:      make(chan models.WorkerCapabilitiesUpdate, 100),
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

	registry := locality.NewDataLocalityRegistry()
	nativePlugins := NewNativePlugins()
	ps := NewPluginServer(registry, nativePlugins, socketPath)
	w, err := NewWorker(ps, lis.Addr().String())
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
	var update models.WorkerCapabilitiesUpdate
	foundInternal := false
	foundStd := false
	timeout := time.After(2 * time.Second)
	for !foundInternal || !foundStd {
		select {
		case update = <-mock.Capabilities:
			if slices.Contains(update.Capabilities, "__internal.identity") {
				foundInternal = true
			}
			if slices.Contains(update.Capabilities, "std/io.print") {
				foundStd = true
			}
		case <-timeout:
			t.Fatal("Timeout waiting for initial internal capabilities update")
		}
	}

	// Register plugin with capabilities
	reg := plugin.PluginRegistration{
		Namespace: "test-ns",
		Language:  "go",
		Schemas: map[string]schema.StepSchemas{
			"test-ns.step1": {},
			"test-ns.step2": {},
		},
	}
	body, _ := json.Marshal(reg)
	_, err = client.DoAction(ctx, &flight.Action{
		Type: plugin.ActionRegisterPlugin,
		Body: body,
	})
	assert.NoError(t, err)

	// Verify capability update reached control plane
	found := false
	timeout = time.After(5 * time.Second)
	for !found {
		select {
		case update := <-mock.Capabilities:
			if slices.Contains(update.Capabilities, "test-ns.step1") {
				found = true
			}
			if found {
				assert.Contains(t, update.Capabilities, "test-ns.step1")
				assert.Contains(t, update.Capabilities, "test-ns.step2")
			}
		case <-timeout:
			t.Fatal("Timeout waiting for capability update")
		}
	}
}

func TestWorker_ProtectInternalNamespace(t *testing.T) {
	mock := &mockControlPlane{
		RegisteredWorkers: make(chan models.WorkerRegistration, 1),
		RegisteredIDs:     make(chan string, 1),
		Capabilities:      make(chan models.WorkerCapabilitiesUpdate, 100),
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

	registry := locality.NewDataLocalityRegistry()
	nativePlugins := NewNativePlugins()
	ps := NewPluginServer(registry, nativePlugins, socketPath)
	w, err := NewWorker(ps, lis.Addr().String())
	require.NoError(t, err)

	go w.Start(ctx)

	// Wait for initial registration containing __internal.identity
	foundInternal := false
	timeout := time.After(5 * time.Second)
	for !foundInternal {
		select {
		case update := <-mock.Capabilities:
			if slices.Contains(update.Capabilities, "__internal.identity") {
				foundInternal = true
			}
		case <-timeout:
			t.Fatal("Timeout waiting for initial capabilities containing __internal.identity")
		}
	}

	// Wait for worker to be fully ready (plugin server started)
	select {
	case <-w.Ready:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for worker ready")
	}

	// Drain any pending internal capability updates
	time.Sleep(100 * time.Millisecond)
	for len(mock.Capabilities) > 0 {
		<-mock.Capabilities
	}

	// Connect as a plugin and try to register an internal capability
	conn, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)

	reg := plugin.PluginRegistration{
		Namespace: "evil-plugin",
		Language:  "go",
		Schemas: map[string]schema.StepSchemas{
			"__internal.identity": {},
		},
	}
	body, _ := json.Marshal(reg)
	res, err := client.DoAction(ctx, &flight.Action{
		Type: plugin.ActionRegisterPlugin,
		Body: body,
	})
	require.NoError(t, err)

	_, err = res.Recv()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PermissionDenied")
}
