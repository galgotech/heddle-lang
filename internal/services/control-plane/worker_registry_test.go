package controlplane

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorkerRegistry_RegisterAndGet(t *testing.T) {
	registry := NewWorkerRegistry()
	defer registry.Close()

	id := "worker-1"
	address := "127.0.0.1:50051"
	uds := "/tmp/heddle.sock"
	labels := map[string]string{"env": "prod"}

	registry.Register(id, address, uds, labels)

	// Small sleep to ensure the manager goroutine processed the request
	time.Sleep(10 * time.Millisecond)

	worker, err := registry.GetWorker(id)
	assert.NoError(t, err)
	assert.NotNil(t, worker)
	assert.Equal(t, id, worker.ID)
	assert.Equal(t, address, worker.Address)
	assert.Equal(t, uds, worker.UDSAddress)
	assert.Equal(t, labels, worker.Labels)
	assert.Equal(t, WorkerHealthy, worker.State)
}

func TestWorkerRegistry_GetWorkerNotFound(t *testing.T) {
	registry := NewWorkerRegistry()
	defer registry.Close()

	worker, err := registry.GetWorker("non-existent")
	assert.Error(t, err)
	assert.Nil(t, worker)
	assert.Equal(t, "worker not found", err.Error())
}

func TestWorkerRegistry_Heartbeat(t *testing.T) {
	registry := NewWorkerRegistry()
	defer registry.Close()

	id := "worker-1"
	registry.Register(id, "addr", "", nil)
	time.Sleep(10 * time.Millisecond)

	w1, _ := registry.GetWorker(id)
	lastSeen := w1.LastSeenAt

	time.Sleep(10 * time.Millisecond)
	err := registry.Heartbeat(id)
	assert.NoError(t, err)

	w2, _ := registry.GetWorker(id)
	assert.True(t, w2.LastSeenAt.After(lastSeen))
}

func TestWorkerRegistry_GetHealthyWorker(t *testing.T) {
	registry := NewWorkerRegistry()
	defer registry.Close()

	// Initially no workers
	w, err := registry.GetHealthyWorker()
	assert.Error(t, err)
	assert.Nil(t, w)

	// Register a worker
	registry.Register("w1", "addr1", "", nil)
	time.Sleep(10 * time.Millisecond)

	w, err = registry.GetHealthyWorker()
	assert.NoError(t, err)
	assert.Equal(t, "w1", w.ID)
}

func TestWorkerRegistry_GetWorkersByCapability(t *testing.T) {
	registry := NewWorkerRegistry()
	defer registry.Close()

	registry.Register("w1", "addr1", "", map[string]string{
		"capability:math.add": "true",
	})
	registry.Register("w2", "addr2", "", map[string]string{
		"capability:math.sub": "true",
	})
	registry.Register("w3", "addr3", "", map[string]string{
		"capability:math.add": "true",
		"capability:math.sub": "true",
	})

	time.Sleep(10 * time.Millisecond)

	// Check math.add
	workers, err := registry.GetWorkersByCapability("math", "add")
	assert.NoError(t, err)
	assert.Len(t, workers, 2)

	// Check math.sub
	workers, err = registry.GetWorkersByCapability("math", "sub")
	assert.NoError(t, err)
	assert.Len(t, workers, 2)

	// Check non-existent capability
	workers, err = registry.GetWorkersByCapability("math", "mul")
	assert.Error(t, err)
	assert.Nil(t, workers)
}
