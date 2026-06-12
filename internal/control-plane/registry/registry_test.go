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
	r := NewWorkerRegistry()
	assert.NotNil(t, r)
	assert.NotNil(t, r.workers)
	assert.Len(t, r.workers, 0)
}

func TestRegister(t *testing.T) {
	r := NewWorkerRegistry()
	reg := models.WorkerRegistration{
		Address: "127.0.0.1:50051",
	}
	r.Register("worker-1", reg)

	r.workersMu.RLock()
	w, ok := r.workers["worker-1"]
	r.workersMu.RUnlock()

	assert.True(t, ok)
	assert.Equal(t, "worker-1", w.GetID())
	assert.Equal(t, reg.Address, w.workerInfo.Registration.Address)
	assert.NotZero(t, w.GetLastSeen())
	assert.Equal(t, 0, w.GetActiveTasks())
	assert.NotNil(t, w.workerInfo.Schemas)
}

func TestUpdateCapabilities(t *testing.T) {
	r := NewWorkerRegistry()

	// Nonexistent worker
	assert.False(t, r.UpdateCapabilities("worker-none", models.WorkerCapabilitiesUpdate{}))

	// Existing worker
	reg := models.WorkerRegistration{Address: "127.0.0.1:50051"}
	r.Register("worker-1", reg)

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

	r.workersMu.RLock()
	w := r.workers["worker-1"]
	r.workersMu.RUnlock()

	w.workerInfo.mu.RLock()
	caps := w.workerInfo.Capabilities
	schemas := w.workerInfo.Schemas
	w.workerInfo.mu.RUnlock()

	assert.ElementsMatch(t, []string{"test.cap1", "test.cap2"}, caps)
	assert.Len(t, schemas, 1)
	assert.Equal(t, "col1", schemas["test.cap1"].Input[0].Name)
}

func TestHeartbeat(t *testing.T) {
	r := NewWorkerRegistry()

	// Nonexistent worker
	assert.False(t, r.Heartbeat("worker-none", 5))

	// Existing worker
	reg := models.WorkerRegistration{Address: "127.0.0.1:50051"}
	r.Register("worker-1", reg)

	// Set lastSeen to sometime in the past to verify it gets updated
	r.workersMu.RLock()
	wInit := r.workers["worker-1"]
	r.workersMu.RUnlock()
	wInit.UpdateHeartbeat(0, time.Now().Add(-1*time.Hour))

	assert.True(t, r.Heartbeat("worker-1", 10))

	r.workersMu.RLock()
	w := r.workers["worker-1"]
	r.workersMu.RUnlock()

	assert.Equal(t, 10, w.GetActiveTasks())
	assert.WithinDuration(t, time.Now(), w.GetLastSeen(), 2*time.Second)
}

func TestFindWorkerStreamForStep(t *testing.T) {
	r := NewWorkerRegistry()

	// No workers
	w := r.FindWorkerByCapability("test.cap")
	assert.Nil(t, w)

	// Register a worker but lastSeen is too old (> 15 seconds)
	r.Register("worker-old", models.WorkerRegistration{})
	r.UpdateCapabilities("worker-old", models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"test.cap"},
	})
	r.workersMu.RLock()
	wOld := r.workers["worker-old"]
	r.workersMu.RUnlock()
	wOld.UpdateHeartbeat(0, time.Now().Add(-20*time.Second))

	w = r.FindWorkerByCapability("test.cap")
	assert.Nil(t, w)

	// Register a worker that is active (< 15 seconds) but doesn't have capability
	r.Register("worker-active-no-cap", models.WorkerRegistration{})
	r.workersMu.RLock()
	wActiveNoCap := r.workers["worker-active-no-cap"]
	r.workersMu.RUnlock()
	wActiveNoCap.UpdateHeartbeat(0, time.Now().Add(-5*time.Second))

	w = r.FindWorkerByCapability("test.cap")
	assert.Nil(t, w)

	// Register a worker that is active (< 15 seconds) and has capability
	r.Register("worker-active", models.WorkerRegistration{})
	r.UpdateCapabilities("worker-active", models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"test.cap"},
		Schemas: map[string]schema.StepSchemas{
			"test.cap": {},
		},
	})
	r.workersMu.RLock()
	wActive := r.workers["worker-active"]
	r.workersMu.RUnlock()
	wActive.UpdateHeartbeat(0, time.Now().Add(-5*time.Second))

	w = r.FindWorkerByCapability("test.cap")
	assert.NotNil(t, w)
	assert.Equal(t, "worker-active", w.GetID())
}

func TestGetRegistryInfo(t *testing.T) {
	r := NewWorkerRegistry()

	// Empty registry
	info := r.GetRegistryInfo()
	assert.Len(t, info.Steps, 0)

	// Worker 1 is active (lastSeen < 30 seconds)
	r.Register("worker-active", models.WorkerRegistration{})
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
	r.Register("worker-inactive", models.WorkerRegistration{})
	r.UpdateCapabilities("worker-inactive", models.WorkerCapabilitiesUpdate{
		Schemas: map[string]schema.StepSchemas{
			"step.inactive": {
				Input: []schema.ColumnSchema{
					{Name: "col2", ArrowType: "bool"},
				},
			},
		},
	})
	r.workersMu.RLock()
	wInactive := r.workers["worker-inactive"]
	r.workersMu.RUnlock()
	wInactive.UpdateHeartbeat(0, time.Now().Add(-40*time.Second))

	info = r.GetRegistryInfo()
	assert.Len(t, info.Steps, 1)
	assert.Contains(t, info.Steps, "step.active")
	assert.NotContains(t, info.Steps, "step.inactive")
	assert.Equal(t, "col1", info.Steps["step.active"].Input[0].Name)
}

func TestWorkflowClientMapping(t *testing.T) {
	r := NewWorkerRegistry()

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
	r := NewWorkerRegistry()

	// 1. Register two workers and update both with "shared.cap"
	r.Register("worker-a", models.WorkerRegistration{})
	r.Register("worker-b", models.WorkerRegistration{})

	r.UpdateCapabilities("worker-a", models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"shared.cap", "unique.a"},
	})
	r.UpdateCapabilities("worker-b", models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"shared.cap", "unique.b"},
	})

	// Make sure both are active (lastSeen is within threshold)
	r.workersMu.RLock()
	wa := r.workers["worker-a"]
	wb := r.workers["worker-b"]
	r.workersMu.RUnlock()
	wa.UpdateHeartbeat(0, time.Now())
	wb.UpdateHeartbeat(0, time.Now())

	// 2. FindWorkerByCapability should find one of them
	found := r.FindWorkerByCapability("shared.cap")
	assert.NotNil(t, found)
	assert.Contains(t, []string{"worker-a", "worker-b"}, found.GetID())

	// 3. Stop worker-a, only worker-b should remain for shared.cap, unique.a should be gone
	r.StopStream("worker-a")
	found = r.FindWorkerByCapability("shared.cap")
	assert.NotNil(t, found)
	assert.Equal(t, "worker-b", found.GetID())

	foundUniqueA := r.FindWorkerByCapability("unique.a")
	assert.Nil(t, foundUniqueA)

	// 4. Re-registration of worker-b clears old capability index for it
	// Worker-b had "shared.cap" and "unique.b"
	r.Register("worker-b", models.WorkerRegistration{})
	// After re-registration, it hasn't updated capabilities yet, so it shouldn't support anything
	found = r.FindWorkerByCapability("shared.cap")
	assert.Nil(t, found)
}
