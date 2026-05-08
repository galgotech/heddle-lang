package manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRegistry_RegisterAndHeartbeat(t *testing.T) {
	r := NewRegistry()

	r.Register("worker-1", "localhost:0", "", map[string]string{"lang": "python"})

	w, err := r.GetHealthyWorker()
	assert.NoError(t, err)
	assert.Equal(t, "worker-1", w.ID)
	assert.Equal(t, WorkerHealthy, w.State)

	// Test heartbeat on non-existent worker
	err = r.Heartbeat("worker-2")
	assert.Error(t, err)
	assert.Equal(t, "worker not found", err.Error())

	// Test heartbeat on existing
	time.Sleep(10 * time.Millisecond) // Let time advance a bit
	lastSeen := w.LastSeenAt

	err = r.Heartbeat("worker-1")
	assert.NoError(t, err)

	w2, _ := r.GetHealthyWorker()
	assert.True(t, w2.LastSeenAt.After(lastSeen))
}

func TestRegistry_StaleWorkerOffline(t *testing.T) {
	r := NewRegistry()

	// Register a worker but manually set its LastSeenAt to far in the past
	r.Register("worker-1", "localhost:0", "", map[string]string{"lang": "python"})
	dr := r.(*DefaultWorkerRegistry)
	dr.mu.Lock()
	dr.workers["worker-1"].LastSeenAt = time.Now().Add(-40 * time.Second)
	dr.mu.Unlock()

	_, err := r.GetHealthyWorker()
	assert.Error(t, err)
	assert.Equal(t, "no healthy workers available", err.Error())
}
