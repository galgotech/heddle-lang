package controlplane

import (
	"testing"
	"time"

	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
	"github.com/stretchr/testify/assert"
)

func TestControlPlane_WorkerRegistration(t *testing.T) {
	wr := NewWorkerRegistry()
	wq := NewWorkQueue()
	dl := NewDataLocalityRegistry()
	cp := NewControlPlane(wr, wq, dl)
	cp.Start()
	defer cp.Stop()

	workerID := "test-worker"
	reg := execution.WorkerRegistration{
		Address:    "localhost:50051",
		UDSAddress: "/tmp/heddle.sock",
		Runtime:    "python",
		Tags:       map[string]string{"gpu": "true"},
	}

	cp.RegisterWorker(reg, workerID)

	// Verify worker is in registry with retry
	var w *Worker
	var err error
	for i := 0; i < 20; i++ {
		w, err = wr.GetWorker(workerID)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.NoError(t, err)
	if w != nil {
		assert.Equal(t, workerID, w.ID)
		assert.Equal(t, "localhost:50051", w.Address)
	}
}

func TestControlPlane_Heartbeat(t *testing.T) {
	wr := NewWorkerRegistry()
	wq := NewWorkQueue()
	dl := NewDataLocalityRegistry()
	cp := NewControlPlane(wr, wq, dl)
	cp.Start()
	defer cp.Stop()

	workerID := "test-worker"
	reg := execution.WorkerRegistration{}
	cp.RegisterWorker(reg, workerID)

	hb := execution.Heartbeat{
		Status: execution.WorkerStatusIdle,
		Load:   0.5,
	}

	var err error
	for range 20 {
		err = cp.Heartbeat(hb, workerID)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.NoError(t, err)
}

func TestControlPlane_TaskStreaming(t *testing.T) {
	wr := NewWorkerRegistry()
	wq := NewWorkQueue()
	dl := NewDataLocalityRegistry()
	cp := NewControlPlane(wr, wq, dl)
	cp.Start()
	defer cp.Stop()

	workerID := "test-worker"
	ch := make(chan *execution.Task, 1)
	cp.RegisterStream(workerID, ch)

	// Verify we can get the stream back
	retCh, ok := cp.GetTaskStream(workerID)
	assert.True(t, ok)
	assert.Equal(t, ch, retCh)

	// Verify unregister
	cp.UnregisterStream(workerID)
	time.Sleep(50 * time.Millisecond) // Give it a moment to process the unregister
	_, ok = cp.GetTaskStream(workerID)
	assert.False(t, ok)
}

func TestControlPlane_ReportTaskUpdate(t *testing.T) {
	wr := NewWorkerRegistry()
	wq := NewWorkQueue()
	dl := NewDataLocalityRegistry()
	cp := NewControlPlane(wr, wq, dl)
	cp.Start()
	defer cp.Stop()

	workerID := "test-worker"
	update := execution.TaskUpdate{
		TaskID:       "task-1",
		Status:       string(execution.TaskStatusDone),
		OutputHandle: "output-1",
	}

	cp.ReportTaskUpdate(update, workerID)

	// Verify data locality
	producer, ok := dl.GetProducer("output-1")
	assert.True(t, ok)
	assert.Equal(t, workerID, producer)
}
