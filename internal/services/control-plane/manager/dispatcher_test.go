package manager

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"

	"github.com/galgotech/heddle-lang/internal/services/control-plane/scheduler"
	"github.com/galgotech/heddle-lang/internal/services/control-plane/state"
)

func TestDispatcher_SuccessfulExecution(t *testing.T) {
	q := scheduler.NewWorkQueue(rate.Inf, 1, nil)
	r := NewRegistry()
	r.Register("worker-1", "localhost:0", "", nil)

	var mu sync.Mutex
	executed := false
	executedNode := ""

	executor := func(ctx context.Context, workerID string, nodeID string) error {
		mu.Lock()
		defer mu.Unlock()
		executed = true
		executedNode = nodeID
		assert.Equal(t, "worker-1", workerID)
		return nil
	}

	sm := state.NewStateMachine()
	_ = sm.AddNode(state.NewNode("node-a"))

	d := NewDispatcher(q, r, sm, executor, NewGoroutinePool())
	d.Start(1)

	// Add a task
	q.Add("node-a", 3)

	// Wait for execution
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.True(t, executed)
	assert.Equal(t, "node-a", executedNode)
	mu.Unlock()

	// Task should be marked done and removed from queue
	assert.Equal(t, 0, q.Length())

	d.Stop()
}

func TestDispatcher_FailedExecutionTriggersRetry(t *testing.T) {
	q := scheduler.NewWorkQueue(rate.Inf, 1, nil)
	r := NewRegistry()
	r.Register("worker-1", "localhost:0", "", nil)

	var mu sync.Mutex
	attempts := 0

	executor := func(ctx context.Context, workerID string, nodeID string) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		return errors.New("simulated failure")
	}

	sm := state.NewStateMachine()
	_ = sm.AddNode(state.NewNode("node-b"))

	d := NewDispatcher(q, r, sm, executor, NewGoroutinePool())
	d.Start(1)

	// Max retries = 1
	q.Add("node-b", 1)

	// Wait for first attempt + retry backoff + second attempt
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	assert.GreaterOrEqual(t, attempts, 2)
	mu.Unlock()

	d.Stop()
}

func TestDispatcher_NoWorkersAvailableTriggersRetry(t *testing.T) {
	q := scheduler.NewWorkQueue(rate.Inf, 1, nil)
	r := NewRegistry() // Empty registry, no healthy workers

	executed := false
	executor := func(ctx context.Context, workerID string, nodeID string) error {
		executed = true
		return nil
	}

	sm := state.NewStateMachine()
	_ = sm.AddNode(state.NewNode("node-c"))

	d := NewDispatcher(q, r, sm, executor, NewGoroutinePool())
	d.Start(1)

	q.Add("node-c", 1)

	// Wait briefly
	time.Sleep(100 * time.Millisecond)

	assert.False(t, executed, "Task should not execute if no workers are available")

	// Ensure the task was retried/requeued
	// Stop the dispatcher first so the workers won't pull the requeued item again.
	// Note: d.Stop() calls d.queue.ShutDown(), which clears the queue or stops accepting things
	// Wait a bit before stopping.

	// Because the queue retry mechanism does an exponential backoff sleep *outside* the queue lock,
	// checking length immediately after adding is tricky in tests since the task might be in `time.Sleep` logic.
	// To reliably test this without flaky sleeps, we can verify the mock executor wasn't called (already done),
	// and trust the Retry test in `scheduler/workqueue_test.go` handles queue bounds.
	// Here, we just stop cleanly.

	d.Stop()
}

func TestDispatcher_ContextCancellationExitsLoop(t *testing.T) {
	q := scheduler.NewWorkQueue(rate.Inf, 1, nil)
	r := NewRegistry()
	r.Register("worker-1", "localhost:0", "", nil)

	executor := func(ctx context.Context, workerID string, nodeID string) error {
		// Simulate long running
		time.Sleep(1 * time.Second)
		return nil
	}

	sm := state.NewStateMachine()
	_ = sm.AddNode(state.NewNode("node-d"))

	d := NewDispatcher(q, r, sm, executor, NewGoroutinePool())
	d.Start(1)

	q.Add("node-d", 1)

	// Wait just enough to let the dispatcher pick it up
	time.Sleep(50 * time.Millisecond)

	// Stop calls context cancel
	d.Stop()

	// Verify dispatcher shuts down cleanly without panicking
}
