package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
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

func TestControlPlane_TaskDispatch(t *testing.T) {
	workerRegistry := NewWorkerRegistry()
	workQueue := NewWorkQueue()
	dataLocalityRegistry := NewDataLocalityRegistry()
	controlPlane := NewControlPlane(workerRegistry, workQueue, dataLocalityRegistry)
	controlPlane.Start()
	defer controlPlane.Stop()

	workerID := "test-worker"
	// Register worker with capability
	controlPlane.RegisterWorker(execution.WorkerRegistration{
		Address: "localhost:50051",
		Tags:    map[string]string{"capability:math.add": "true"},
	}, workerID)

	// Manually register a task stream for the worker
	taskCh := make(chan *execution.Task, 1)
	controlPlane.registerStreamCh <- registerStreamRequest{
		workerID: workerID,
		taskCh:   taskCh,
	}

	task := &execution.Task{
		ID: "task-1",
		Step: &ir.StepInstruction{
			Call: []string{"math", "add"},
		},
	}

	// Dispatch task in background
	updateCh := make(chan execution.TaskUpdate)
	go func() {
		update, err := controlPlane.dispatchTask(context.Background(), task)
		if err != nil {
			t.Errorf("dispatchTask failed: %v", err)
		}
		updateCh <- update
	}()

	// Worker (us in the test) should receive the task
	select {
	case receivedTask := <-taskCh:
		assert.Equal(t, task.ID, receivedTask.ID)
	case <-time.After(2 * time.Second):
		t.Fatal("Worker did not receive task")
	}

	// Send update back
	update := execution.TaskUpdate{
		TaskID:       task.ID,
		Status:       string(execution.TaskStatusDone),
		OutputHandle: "mem-123",
		Timestamp:    time.Now(),
	}
	controlPlane.reportUpdateCh <- update

	// ControlPlane should return the update from dispatchTask
	select {
	case receivedUpdate := <-updateCh:
		assert.Equal(t, update.TaskID, receivedUpdate.TaskID)
		assert.Equal(t, update.Status, receivedUpdate.Status)
		assert.Equal(t, update.OutputHandle, receivedUpdate.OutputHandle)
	case <-time.After(2 * time.Second):
		t.Fatal("ControlPlane did not return task update")
	}
}
