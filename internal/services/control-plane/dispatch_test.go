package controlplane

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
	"github.com/galgotech/heddle-lang/pkg/runtime/state"
	"github.com/stretchr/testify/assert"
)

type mockDoExchangeServer struct {
	flight.FlightService_DoExchangeServer
	ctx context.Context
	in  chan *flight.FlightData
	out chan *flight.FlightData
}

func (m *mockDoExchangeServer) Context() context.Context {
	return m.ctx
}

func (m *mockDoExchangeServer) Send(data *flight.FlightData) error {
	m.out <- data
	return nil
}

func (m *mockDoExchangeServer) Recv() (*flight.FlightData, error) {
	select {
	case data := <-m.in:
		return data, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func TestControlPlane_DispatchTask(t *testing.T) {
	wr := NewWorkerRegistry()
	wq := NewWorkQueue()
	dl := NewDataLocalityRegistry()
	cp := NewControlPlane(wr, wq, dl)
	cp.Start()
	defer cp.Stop()

	workerID := "test-worker"
	// Register worker with capability
	wr.Register(workerID, "localhost:50051", "", map[string]string{
		"capability:math.add": "true",
	})

	// Wait for registration to process
	time.Sleep(100 * time.Millisecond)

	// Simulate Exchange
	hCtx := state.NewHeddleContext(context.Background(), state.Credentials{}, state.Metadata{
		Values: map[string]any{"worker_id": workerID},
	})

	serverStream := &mockDoExchangeServer{
		ctx: hCtx,
		in:  make(chan *flight.FlightData, 10),
		out: make(chan *flight.FlightData, 10),
	}

	go func() {
		_ = cp.Exchange(serverStream)
	}()

	// Wait for Exchange to register
	time.Sleep(100 * time.Millisecond)

	task := &execution.Task{
		ID: "task-1",
		Step: &ir.StepInstruction{
			Call: []string{"math", "add"},
		},
	}

	// Dispatch task in background because it blocks until update
	updateCh := make(chan execution.TaskUpdate)
	go func() {
		update, err := cp.dispatchTask(context.Background(), task)
		if err != nil {
			t.Errorf("dispatchTask failed: %v", err)
		}
		updateCh <- update
	}()

	// Worker should receive the task
	select {
	case data := <-serverStream.out:
		var receivedTask execution.Task
		err := json.Unmarshal(data.DataBody, &receivedTask)
		assert.NoError(t, err)
		assert.Equal(t, task.ID, receivedTask.ID)
	case <-time.After(2 * time.Second):
		t.Fatal("Worker did not receive task")
	}

	// Worker sends update
	update := execution.TaskUpdate{
		TaskID:       task.ID,
		Status:       string(execution.TaskStatusDone),
		OutputHandle: "mem-123",
		Timestamp:    time.Now(),
	}
	body, _ := json.Marshal(update)
	serverStream.in <- &flight.FlightData{DataBody: body}

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
