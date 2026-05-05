package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"

	"github.com/galgotech/heddle-lang/services/control-plane/pkg/scheduler"
	"github.com/galgotech/heddle-lang/services/control-plane/pkg/state"
)

// MockDispatcher provides a manual mock for the Dispatcher interface,
// allowing deterministic control over task execution loops in tests.
type MockDispatcher struct {
	OnStart    func(concurrency int)
	OnStop     func()
	OnDispatch func(ctx context.Context, task *scheduler.Task) error
}

func (m *MockDispatcher) Start(concurrency int) {
	if m.OnStart != nil {
		m.OnStart(concurrency)
	}
}

func (m *MockDispatcher) Stop() {
	if m.OnStop != nil {
		m.OnStop()
	}
}

func (m *MockDispatcher) Dispatch(ctx context.Context, task *scheduler.Task) error {
	if m.OnDispatch != nil {
		return m.OnDispatch(ctx, task)
	}
	return nil
}

// MockLocalityRegistry provides a manual mock for the DataLocalityRegistry interface,
// enabling simulation of cluster-wide data distribution and invalidation.
type MockLocalityRegistry struct {
	OnRegisterOutput func(resourceKey string, workerID string)
	OnGetProducer    func(resourceKey string) (string, bool)
	OnInvalidate     func(resourceKey string)
}

func (m *MockLocalityRegistry) RegisterOutput(resourceKey string, workerID string) {
	if m.OnRegisterOutput != nil {
		m.OnRegisterOutput(resourceKey, workerID)
	}
}

func (m *MockLocalityRegistry) GetProducer(resourceKey string) (string, bool) {
	if m.OnGetProducer != nil {
		return m.OnGetProducer(resourceKey)
	}
	return "", false
}

func (m *MockLocalityRegistry) Invalidate(resourceKey string) {
	if m.OnInvalidate != nil {
		m.OnInvalidate(resourceKey)
	}
}

// TestWorkerFailureScenario simulates a worker crash mid-execution and verifies
// that the DAG state machine correctly transitions the node to a Failed state.
func TestWorkerFailureScenario(t *testing.T) {
	q := scheduler.NewWorkQueue(rate.Inf, 1, nil)
	r := NewRegistry()
	sm := state.NewStateMachine()

	nodeID := "node-failure-test"
	_ = sm.AddNode(state.NewNode(nodeID))

	r.Register("worker-1", "localhost:0", "", nil)

	// Simulate a network timeout or worker crash via the executor.
	failureErr := errors.New("worker timed out")
	executor := func(ctx context.Context, workerID string, nodeID string) error {
		return failureErr
	}

	d := NewDispatcher(q, r, sm, executor)
	d.Start(1)

	// Add task with 0 retries to trigger immediate terminal failure.
	q.Add(nodeID, 0)

	// Allow the dispatcher loop to process the failure.
	time.Sleep(200 * time.Millisecond)

	// Assert: The DAG state machine must reflect the terminal failure.
	node, err := sm.GetNode(nodeID)
	assert.NoError(t, err)
	assert.Equal(t, state.Failed, node.State)
	assert.Equal(t, failureErr, node.Error)

	d.Stop()
}

// TestDataLocalityInvalidation verifies that the registry can correctly purge
// entries, preventing the scheduler from attempting to route tasks to stale data.
func TestDataLocalityInvalidation(t *testing.T) {
	registry := NewDataLocalityRegistry()

	resource := "shared-buffer-001"
	worker := "worker-alpha"

	registry.RegisterOutput(resource, worker)

	producer, ok := registry.GetProducer(resource)
	assert.True(t, ok)
	assert.Equal(t, worker, producer)

	// Invalidate the resource (e.g., following a worker heartbeat failure).
	registry.Invalidate(resource)

	_, ok = registry.GetProducer(resource)
	assert.False(t, ok, "Registry must not return producer for invalidated resource")
}
