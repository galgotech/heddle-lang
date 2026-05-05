package manager

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"

	"github.com/galgotech/heddle-lang/services/control-plane/pkg/scheduler"
	"github.com/galgotech/heddle-lang/services/control-plane/pkg/state"
)

// TestCHASM_WorkerTimeoutAndRetryIsolation validates that failures are isolated
// to the specific node and that retries do not trigger re-execution of the entire DAG.
func TestCHASM_WorkerTimeoutAndRetryIsolation(t *testing.T) {
	q := scheduler.NewWorkQueue(rate.Inf, 1, nil)
	r := NewRegistry()
	sm := state.NewStateMachine()

	nodeA := "node-a"
	nodeB := "node-b"
	_ = sm.AddNode(state.NewNode(nodeA))
	_ = sm.AddNode(state.NewNode(nodeB))

	r.Register("worker-1", "localhost:0", "", nil)

	var mu sync.Mutex
	executions := make(map[string]int)

	executor := func(ctx context.Context, workerID string, nodeID string) error {
		mu.Lock()
		executions[nodeID]++
		attempt := executions[nodeID]
		mu.Unlock()

		if nodeID == nodeB && attempt == 1 {
			// Simulate a transient failure on node B's first attempt
			return errors.New("transient worker timeout")
		}
		return nil
	}

	d := NewDispatcher(q, r, sm, executor, NewGoroutinePool())
	d.Start(1)

	// Step 1: Execute node A successfully
	q.Add(nodeA, 0)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 1, executions[nodeA], "Node A should execute exactly once")
	mu.Unlock()

	nodeAState, _ := sm.GetNode(nodeA)
	assert.Equal(t, state.Completed, nodeAState.GetState())

	// Step 2: Execute node B with a transient failure
	q.Add(nodeB, 1) // 1 retry allowed
	
	// Wait for first attempt (fail) + backoff (~200ms) + second attempt (success)
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 2, executions[nodeB], "Node B should execute twice (initial + retry)")
	assert.Equal(t, 1, executions[nodeA], "Node A must NOT have been re-executed (CHASM Isolation)")
	mu.Unlock()

	nodeBState, _ := sm.GetNode(nodeB)
	assert.Equal(t, state.Completed, nodeBState.GetState())

	d.Stop()
}

// TestCHASM_AccidentalReexecutionFailure ensures that terminal failures in one node
// do not corrupt the state of previously completed nodes.
func TestCHASM_AccidentalReexecutionFailure(t *testing.T) {
	q := scheduler.NewWorkQueue(rate.Inf, 1, nil)
	r := NewRegistry()
	sm := state.NewStateMachine()

	nodeA := "node-completed"
	nodeB := "node-terminal-failure"
	_ = sm.AddNode(state.NewNode(nodeA))
	_ = sm.AddNode(state.NewNode(nodeB))

	r.Register("worker-1", "localhost:0", "", nil)

	executor := func(ctx context.Context, workerID string, nodeID string) error {
		if nodeID == nodeB {
			return errors.New("critical step failure")
		}
		return nil
	}

	d := NewDispatcher(q, r, sm, executor, NewGoroutinePool())
	d.Start(1)

	// Complete node A
	q.Add(nodeA, 0)
	time.Sleep(100 * time.Millisecond)

	// Fail node B terminally
	q.Add(nodeB, 0) // 0 retries
	time.Sleep(100 * time.Millisecond)

	// Assertions
	nodeAState, _ := sm.GetNode(nodeA)
	assert.Equal(t, state.Completed, nodeAState.GetState())

	nodeBState, _ := sm.GetNode(nodeB)
	assert.Equal(t, state.Failed, nodeBState.GetState())

	// Verify history doesn't show any accidental transitions for nodeA
	history := sm.GetHistory()
	for _, snap := range history {
		if snap.ID == nodeA {
			assert.Equal(t, "Completed", snap.State)
		}
	}

	d.Stop()
}

// TestCHASM_ConcurrentChaosAndRaceDetector subjects the dispatcher to heavy load
// with stochastic failures to audit for race conditions and goroutine leaks.
func TestCHASM_ConcurrentChaosAndRaceDetector(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		numTasks    = 100
		concurrency = 10
	)

	q := scheduler.NewWorkQueue(rate.Inf, 10, nil)
	r := NewRegistry()
	sm := state.NewStateMachine()

	r.Register("worker-1", "localhost:0", "", nil)
	r.Register("worker-2", "localhost:0", "", nil)

	var activeExecutions int32
	var totalAttempts int32

	executor := func(ctx context.Context, workerID string, nodeID string) error {
		atomic.AddInt32(&activeExecutions, 1)
		defer atomic.AddInt32(&activeExecutions, -1)
		atomic.AddInt32(&totalAttempts, 1)

		// Inject artificial latency
		time.Sleep(10 * time.Millisecond)

		// Inject stochastic failure (10% chance)
		if time.Now().UnixNano()%10 == 0 {
			return errors.New("stochastic chaos failure")
		}
		return nil
	}

	d := NewDispatcher(q, r, sm, executor, NewGoroutinePool())
	d.Start(concurrency)

	for i := range numTasks {
		nodeID := "node-" + string(rune(i))
		_ = sm.AddNode(state.NewNode(nodeID))
		q.Add(nodeID, 3) // Allow some retries
	}

	// Wait for all tasks to be processed (either Completed or Failed)
	// We check if queue is empty and no active executions
	assert.Eventually(t, func() bool {
		mu := sync.Mutex{}
		mu.Lock()
		defer mu.Unlock()
		
		done := true
		for i := range numTasks {
			nodeID := "node-" + string(rune(i))
			node, _ := sm.GetNode(nodeID)
			if node.GetState() != state.Completed && node.GetState() != state.Failed {
				done = false
				break
			}
		}
		return done && q.Length() == 0 && atomic.LoadInt32(&activeExecutions) == 0
	}, 10*time.Second, 100*time.Millisecond)

	d.Stop()

	// If this test passes with -race, it validates concurrency stability.
	t.Logf("Completed stress test with %d total attempts for %d nodes", atomic.LoadInt32(&totalAttempts), numTasks)
}
