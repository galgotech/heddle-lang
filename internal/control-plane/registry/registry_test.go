package registry

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

// mockExchangeServer implements transport.ExchangeStream
type mockExchangeServer struct {
	recvChan chan *transport.FlightData
	errChan  chan error
}

func (m *mockExchangeServer) Send(data *transport.FlightData) error {
	return nil
}

func (m *mockExchangeServer) Recv() (*transport.FlightData, error) {
	select {
	case data := <-m.recvChan:
		return data, nil
	case err, ok := <-m.errChan:
		if !ok {
			return nil, io.EOF
		}
		return nil, err
	}
}

func TestNewWorkerRegistry(t *testing.T) {
	r := NewNodeRegistry()
	assert.NotNil(t, r)
	assert.NotNil(t, r.nodes)
	assert.Len(t, r.nodes, 0)
}

func TestRegister(t *testing.T) {
	r := NewNodeRegistry()
	reg := models.WorkerRegistration{
		Address: "127.0.0.1:50051",
	}
	r.RegisterNode("worker-1", NodeTypeWorker, reg)

	r.nodesMu.RLock()
	w, ok := r.nodes["worker-1"]
	r.nodesMu.RUnlock()

	assert.True(t, ok)
	assert.Equal(t, "worker-1", w.GetID())
	assert.Equal(t, reg.Address, w.workerInfo.Registration.Address)
	assert.NotZero(t, w.GetLastSeen())
	assert.Equal(t, 0, w.GetActiveTasks())
	assert.NotNil(t, w.workerInfo.Schemas)
}

func TestUpdateCapabilities(t *testing.T) {
	r := NewNodeRegistry()

	// Nonexistent worker
	assert.False(t, r.UpdateCapabilities("worker-none", models.WorkerCapabilitiesUpdate{}))

	// Existing worker
	reg := models.WorkerRegistration{Address: "127.0.0.1:50051"}
	r.RegisterNode("worker-1", NodeTypeWorker, reg)

	update := models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"test.cap1", "test.cap2"},
		Schemas: map[string]schema.StepSchemas{
			"test.cap1": {
				Input: []schema.ColumnSchema{
					{Name: "col1", ArrowType: "int64"},
				},
				Output: []schema.ColumnSchema{},
			},
		},
	}

	assert.True(t, r.UpdateCapabilities("worker-1", update))

	r.nodesMu.RLock()
	w := r.nodes["worker-1"]
	r.nodesMu.RUnlock()

	w.workerInfo.mu.RLock()
	caps := w.workerInfo.Capabilities
	schemas := w.workerInfo.Schemas
	w.workerInfo.mu.RUnlock()

	assert.ElementsMatch(t, []string{"test.cap1", "test.cap2"}, caps)
	assert.Len(t, schemas, 1)
	assert.Equal(t, "col1", schemas["test.cap1"].Input[0].Name)
}

func TestHeartbeat(t *testing.T) {
	r := NewNodeRegistry()

	// Nonexistent worker
	assert.False(t, r.Heartbeat("worker-none", 5))

	// Existing worker
	reg := models.WorkerRegistration{Address: "127.0.0.1:50051"}
	r.RegisterNode("worker-1", NodeTypeWorker, reg)

	// Set lastSeen to sometime in the past to verify it gets updated
	r.nodesMu.RLock()
	wInit := r.nodes["worker-1"]
	r.nodesMu.RUnlock()
	wInit.UpdateHeartbeat(0, time.Now().Add(-1*time.Hour))

	assert.True(t, r.Heartbeat("worker-1", 10))

	r.nodesMu.RLock()
	w := r.nodes["worker-1"]
	r.nodesMu.RUnlock()

	assert.Equal(t, 10, w.GetActiveTasks())
	assert.WithinDuration(t, time.Now(), w.GetLastSeen(), 2*time.Second)
}

func TestFindWorkerStreamForStep(t *testing.T) {
	r := NewNodeRegistry()

	// No workers
	w := r.FindWorkerByCapability("test.cap")
	assert.Nil(t, w)

	// Register a worker but lastSeen is too old (> 15 seconds)
	r.RegisterNode("worker-old", NodeTypeWorker, models.WorkerRegistration{})
	r.UpdateCapabilities("worker-old", models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"test.cap"},
	})
	r.nodesMu.RLock()
	wOld := r.nodes["worker-old"]
	r.nodesMu.RUnlock()
	wOld.UpdateHeartbeat(0, time.Now().Add(-20*time.Second))

	w = r.FindWorkerByCapability("test.cap")
	assert.Nil(t, w)

	// Register a worker that is active (< 15 seconds) but doesn't have capability
	r.RegisterNode("worker-active-no-cap", NodeTypeWorker, models.WorkerRegistration{})
	r.nodesMu.RLock()
	wActiveNoCap := r.nodes["worker-active-no-cap"]
	r.nodesMu.RUnlock()
	wActiveNoCap.UpdateHeartbeat(0, time.Now().Add(-5*time.Second))

	w = r.FindWorkerByCapability("test.cap")
	assert.Nil(t, w)

	// Register a worker that is active (< 15 seconds) and has capability
	r.RegisterNode("worker-active", NodeTypeWorker, models.WorkerRegistration{})
	r.UpdateCapabilities("worker-active", models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"test.cap"},
		Schemas: map[string]schema.StepSchemas{
			"test.cap": {},
		},
	})
	r.nodesMu.RLock()
	wActive := r.nodes["worker-active"]
	r.nodesMu.RUnlock()
	wActive.UpdateHeartbeat(0, time.Now().Add(-5*time.Second))

	w = r.FindWorkerByCapability("test.cap")
	assert.NotNil(t, w)
	assert.Equal(t, "worker-active", w.GetID())
}

func TestGetRegistryInfo(t *testing.T) {
	r := NewNodeRegistry()

	// Empty registry
	info := r.GetRegistryInfo()
	assert.Len(t, info.Steps, 0)

	// Worker 1 is active (lastSeen < 30 seconds)
	r.RegisterNode("worker-active", NodeTypeWorker, models.WorkerRegistration{})
	r.UpdateCapabilities("worker-active", models.WorkerCapabilitiesUpdate{
		Schemas: map[string]schema.StepSchemas{
			"step.active": {
				Input: []schema.ColumnSchema{
					{Name: "col1", ArrowType: "utf8"},
				},
			},
		},
	})

	// Worker 2 is inactive (lastSeen > 30 seconds)
	r.RegisterNode("worker-inactive", NodeTypeWorker, models.WorkerRegistration{})
	r.UpdateCapabilities("worker-inactive", models.WorkerCapabilitiesUpdate{
		Schemas: map[string]schema.StepSchemas{
			"step.inactive": {
				Input: []schema.ColumnSchema{
					{Name: "col2", ArrowType: "bool"},
				},
			},
		},
	})
	r.nodesMu.RLock()
	wInactive := r.nodes["worker-inactive"]
	r.nodesMu.RUnlock()
	wInactive.UpdateHeartbeat(0, time.Now().Add(-40*time.Second))

	info = r.GetRegistryInfo()
	assert.Len(t, info.Steps, 1)
	assert.Contains(t, info.Steps, "step.active")
	assert.NotContains(t, info.Steps, "step.inactive")
	assert.Equal(t, "col1", info.Steps["step.active"].Input[0].Name)
}

func TestWorkflowClientMapping(t *testing.T) {
	r := NewNodeRegistry()

	// Initially should not be found
	_, ok := r.GetClientIDForWorkflow("wf-1")
	assert.False(t, ok)

	// Register mapping
	r.RegisterWorkflowClient("wf-1", "client-1")
	clientID, ok := r.GetClientIDForWorkflow("wf-1")
	assert.True(t, ok)
	assert.Equal(t, "client-1", clientID)

	// Deregister mapping
	r.DeregisterWorkflowClient("wf-1")
	_, ok = r.GetClientIDForWorkflow("wf-1")
	assert.False(t, ok)
}

func TestReverseIndexCapabilities(t *testing.T) {
	r := NewNodeRegistry()

	// 1. Register two workers and update both with "shared.cap"
	r.RegisterNode("worker-a", NodeTypeWorker, models.WorkerRegistration{})
	r.RegisterNode("worker-b", NodeTypeWorker, models.WorkerRegistration{})

	r.UpdateCapabilities("worker-a", models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"shared.cap", "unique.a"},
	})
	r.UpdateCapabilities("worker-b", models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"shared.cap", "unique.b"},
	})

	// Make sure both are active (lastSeen is within threshold)
	r.nodesMu.RLock()
	wa := r.nodes["worker-a"]
	wb := r.nodes["worker-b"]
	r.nodesMu.RUnlock()
	wa.UpdateHeartbeat(0, time.Now())
	wb.UpdateHeartbeat(0, time.Now())

	// 2. FindWorkerByCapability should find one of them
	found := r.FindWorkerByCapability("shared.cap")
	assert.NotNil(t, found)
	assert.Contains(t, []string{"worker-a", "worker-b"}, found.GetID())

	// 3. Deregister worker-a, only worker-b should remain for shared.cap, unique.a should be gone
	r.DeregisterNode("worker-a")
	found = r.FindWorkerByCapability("shared.cap")
	assert.NotNil(t, found)
	assert.Equal(t, "worker-b", found.GetID())

	foundUniqueA := r.FindWorkerByCapability("unique.a")
	assert.Nil(t, foundUniqueA)

	// 4. Re-registration of worker-b clears old capability index for it
	// Worker-b had "shared.cap" and "unique.b"
	r.RegisterNode("worker-b", NodeTypeWorker, models.WorkerRegistration{})
	// After re-registration, it hasn't updated capabilities yet, so it shouldn't support anything
	found = r.FindWorkerByCapability("shared.cap")
	assert.Nil(t, found)
}

func TestGetWorker(t *testing.T) {
	r := NewNodeRegistry()

	// Retrieve nonexistent worker
	w, ok := r.GetNode("nonexistent")
	assert.False(t, ok)
	assert.Nil(t, w)

	// Register a worker and retrieve it
	reg := models.WorkerRegistration{Address: "127.0.0.1:50051"}
	r.RegisterNode("worker-1", NodeTypeWorker, reg)

	w, ok = r.GetNode("worker-1")
	assert.True(t, ok)
	assert.NotNil(t, w)
	assert.Equal(t, "worker-1", w.GetID())
}
