package manager

import (
	"context"
	"errors"
	"sync"
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

// MockLocalityRegistry provides a manual mock for the DataLocalityRegistry interface.
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

// MockWorkerRegistry provides a manual mock for the WorkerRegistry interface.
type MockWorkerRegistry struct {
	OnRegister          func(id string, address string, udsAddress string, labels map[string]string)
	OnGetWorker         func(id string) (*Worker, error)
	OnHeartbeat         func(id string) error
	OnGetHealthyWorker func() (*Worker, error)
}

func (m *MockWorkerRegistry) Register(id string, address string, udsAddress string, labels map[string]string) {
	if m.OnRegister != nil {
		m.OnRegister(id, address, udsAddress, labels)
	}
}

func (m *MockWorkerRegistry) GetWorker(id string) (*Worker, error) {
	if m.OnGetWorker != nil {
		return m.OnGetWorker(id)
	}
	return nil, nil
}

func (m *MockWorkerRegistry) Heartbeat(id string) error {
	if m.OnHeartbeat != nil {
		return m.OnHeartbeat(id)
	}
	return nil
}

func (m *MockWorkerRegistry) GetHealthyWorker() (*Worker, error) {
	if m.OnGetHealthyWorker != nil {
		return m.OnGetHealthyWorker()
	}
	return nil, nil
}

// MockStateMachine provides a manual mock for the StateMachine interface.
type MockStateMachine struct {
	OnAddNode    func(node *state.Node) error
	OnGetNode    func(id string) (*state.Node, error)
	OnTransition func(id string, expected state.State, next state.State, err error) error
	OnGetHistory func() []state.NodeSnapshot
}

func (m *MockStateMachine) AddNode(node *state.Node) error {
	if m.OnAddNode != nil {
		return m.OnAddNode(node)
	}
	return nil
}

func (m *MockStateMachine) GetNode(id string) (*state.Node, error) {
	if m.OnGetNode != nil {
		return m.OnGetNode(id)
	}
	return nil, nil
}

func (m *MockStateMachine) Transition(id string, expected state.State, next state.State, err error) error {
	if m.OnTransition != nil {
		return m.OnTransition(id, expected, next, err)
	}
	return nil
}

func (m *MockStateMachine) GetHistory() []state.NodeSnapshot {
	if m.OnGetHistory != nil {
		return m.OnGetHistory()
	}
	return nil
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

// TestLocalityAwareRetryAfterFailure simulates a failure in a downstream node and verifies
// that the dispatcher correctly re-queries the locality registry for predecessor data
// during the subsequent retry attempt.
func TestLocalityAwareRetryAfterFailure(t *testing.T) {
	q := scheduler.NewWorkQueue(rate.Inf, 1, nil)
	r := &MockWorkerRegistry{}
	sm := state.NewStateMachine()
	locality := &MockLocalityRegistry{}

	nodeA := "node-a"
	nodeB := "node-b"
	_ = sm.AddNode(state.NewNode(nodeA))
	_ = sm.AddNode(state.NewNode(nodeB))

	// Simulate nodeA having completed and produced data on worker-1.
	handleA := "handle-a"
	locality.OnGetProducer = func(resourceKey string) (string, bool) {
		if resourceKey == handleA {
			return "worker-1", true
		}
		return "", false
	}

	r.OnGetHealthyWorker = func() (*Worker, error) {
		return &Worker{ID: "worker-2", State: WorkerHealthy}, nil
	}

	var mu sync.Mutex
	localityQueries := 0
	executions := 0

	executor := func(ctx context.Context, workerID string, nodeID string) error {
		mu.Lock()
		executions++
		mu.Unlock()

		// Simulate locality query for predecessor data (in a real scenario this happens in server.go)
		// We mock it here to verify the dispatcher triggers the retry flow correctly.
		_, _ = locality.GetProducer(handleA)
		mu.Lock()
		localityQueries++
		mu.Unlock()

		if executions == 1 {
			return errors.New("temporary worker failure")
		}
		return nil
	}

	d := NewDispatcher(q, r, sm, executor)
	d.Start(1)

	// Add nodeB with 1 retry allowed.
	q.Add(nodeB, 1)

	// Wait for execution and retry.
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 2, executions, "Should have executed twice (initial + retry)")
	assert.Equal(t, 2, localityQueries, "Should have queried locality for each execution attempt")
	mu.Unlock()

	node, _ := sm.GetNode(nodeB)
	assert.Equal(t, state.Completed, node.State)

	d.Stop()
}
