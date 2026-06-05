package registry

import (
	"io"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

// mockExchangeServer implements flight.FlightService_DoExchangeServer
type mockExchangeServer struct {
	flight.FlightService_DoExchangeServer
	recvChan chan *flight.FlightData
	errChan  chan error
}

func (m *mockExchangeServer) Recv() (*flight.FlightData, error) {
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

	r.wokersMu.RLock()
	w, ok := r.workers["worker-1"]
	r.wokersMu.RUnlock()

	assert.True(t, ok)
	assert.Equal(t, "worker-1", w.GetID())
	assert.Equal(t, reg.Address, w.workerInfo.Registration.Address)
	assert.NotZero(t, w.lastSeen)
	assert.Equal(t, 0, w.activeTasks)
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

	r.wokersMu.RLock()
	w := r.workers["worker-1"]
	r.wokersMu.RUnlock()

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
	r.wokersMu.Lock()
	r.workers["worker-1"].lastSeen = time.Now().Add(-1 * time.Hour)
	r.wokersMu.Unlock()

	assert.True(t, r.Heartbeat("worker-1", 10))

	r.wokersMu.RLock()
	w := r.workers["worker-1"]
	r.wokersMu.RUnlock()

	assert.Equal(t, 10, w.activeTasks)
	assert.WithinDuration(t, time.Now(), w.lastSeen, 2*time.Second)
}

func TestFindWorkerStreamForStep(t *testing.T) {
	r := NewWorkerRegistry()

	// No workers
	w := r.FindWorkerStreamForStep("test.cap")
	assert.Nil(t, w)

	// Register a worker but lastSeen is too old (> 15 seconds)
	r.Register("worker-old", models.WorkerRegistration{})
	r.wokersMu.Lock()
	r.workers["worker-old"].lastSeen = time.Now().Add(-20 * time.Second)
	r.wokersMu.Unlock()

	w = r.FindWorkerStreamForStep("test.cap")
	assert.Nil(t, w)

	// Register a worker that is active (< 15 seconds)
	r.Register("worker-active", models.WorkerRegistration{})
	r.wokersMu.Lock()
	r.workers["worker-active"].lastSeen = time.Now().Add(-5 * time.Second)
	r.wokersMu.Unlock()

	w = r.FindWorkerStreamForStep("test.cap")
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
	r.wokersMu.Lock()
	r.workers["worker-inactive"].lastSeen = time.Now().Add(-40 * time.Second)
	r.wokersMu.Unlock()

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
