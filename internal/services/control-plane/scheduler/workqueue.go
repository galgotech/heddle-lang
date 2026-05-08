package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Task represents a unit of work within the scheduler, maintaining execution state
// and retry metadata for deterministic workflow orchestration.
type Task struct {
	NodeID      string
	Retries     int
	MaxRetries  int
	LastFailure time.Time
}

// AoTAnalyzer (Ahead-of-Time) defines the contract for inspecting and prioritizing
// tasks before they are committed to the execution queue.
type AoTAnalyzer interface {
	Analyze(task *Task) (priority int, group string)
}

// DefaultAnalyzer provides a no-op implementation of AoTAnalyzer, treating
// all tasks with uniform priority and group assignments.
type DefaultAnalyzer struct{}

func (d *DefaultAnalyzer) Analyze(task *Task) (int, string) {
	return 0, "default"
}

// WorkQueue implements a highly concurrent, state-aware task distribution system.
// It utilizes a Kubernetes-inspired 'dirty/processing' state machine to ensure
// that any individual task is never executed by more than one worker simultaneously,
// while guaranteeing that updates occurring during processing are eventually handled.
type WorkQueue struct {
	mu   sync.Mutex
	cond *sync.Cond

	queue      []*Task
	processing map[string]*Task
	dirty      map[string]*Task

	rateLimiter *rate.Limiter
	analyzer    AoTAnalyzer

	taskPool sync.Pool

	shuttingDown bool
}

// NewWorkQueue initializes a WorkQueue with token-bucket rate limiting and
// a task object pool to minimize heap allocations.
func NewWorkQueue(qps rate.Limit, burst int, analyzer AoTAnalyzer) *WorkQueue {
	if analyzer == nil {
		analyzer = &DefaultAnalyzer{}
	}

	wq := &WorkQueue{
		queue:       make([]*Task, 0),
		processing:  make(map[string]*Task),
		dirty:       make(map[string]*Task),
		rateLimiter: rate.NewLimiter(qps, burst),
		analyzer:    analyzer,
		taskPool: sync.Pool{
			New: func() any {
				return &Task{}
			},
		},
	}
	wq.cond = sync.NewCond(&wq.mu)
	return wq
}

// Add enqueues a node ID for processing. If the node is already in the 'dirty' set,
// the call is ignored as a pending execution is already scheduled. If the node is
// currently being processed, it is marked 'dirty' to trigger a re-sync upon completion.
func (wq *WorkQueue) Add(nodeID string, maxRetries int) {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.shuttingDown {
		return
	}

	if _, exists := wq.dirty[nodeID]; exists {
		// Already in the dirty set; a pending execution is already scheduled.
		return
	}

	// Retrieve a task object from the pool to minimize heap allocations and GC pressure.
	task := wq.taskPool.Get().(*Task)
	task.NodeID = nodeID
	task.Retries = 0
	task.MaxRetries = maxRetries
	task.LastFailure = time.Time{}

	wq.dirty[nodeID] = task

	if _, processing := wq.processing[nodeID]; !processing {
		wq.queue = append(wq.queue, task)
		wq.cond.Signal()
	}
}

// AddRateLimited enqueues a task after successfully acquiring a token from the rate limiter.
// This blocks if the global throughput limit has been reached.
func (wq *WorkQueue) AddRateLimited(ctx context.Context, nodeID string, maxRetries int) error {
	if err := wq.rateLimiter.Wait(ctx); err != nil {
		return err
	}
	wq.Add(nodeID, maxRetries)
	return nil
}

// Get retrieves the next task from the queue, blocking until an item is available
// or the queue initiates shutdown. Upon return, the task is moved to the 'processing' state.
func (wq *WorkQueue) Get() (*Task, bool) {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	for len(wq.queue) == 0 && !wq.shuttingDown {
		wq.cond.Wait()
	}

	if len(wq.queue) == 0 {
		return nil, true // Queue is shutting down; return nil to signal termination.
	}

	task := wq.queue[0]
	wq.queue = wq.queue[1:]

	wq.processing[task.NodeID] = task
	delete(wq.dirty, task.NodeID)

	return task, false
}

// Done signals the completion of a task's execution. If the task was marked 'dirty'
// while processing, it is immediately re-enqueued. Otherwise, the task object is
// recycled into the pool to reduce GC pressure.
func (wq *WorkQueue) Done(task *Task) {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	delete(wq.processing, task.NodeID)

	// Check if the task was marked dirty during processing to trigger a re-sync.
	if dirtyTask, exists := wq.dirty[task.NodeID]; exists {
		wq.queue = append(wq.queue, dirtyTask)
		wq.cond.Signal()
	} else {
		// Task completed without further updates; recycle object into the pool.
		task.NodeID = ""
		wq.taskPool.Put(task)
	}
}

// Retry handles task failure by scheduling a re-execution with exponential backoff.
// If MaxRetries is exceeded, the task is dropped. The backoff wait is executed
// in a background goroutine to prevent blocking the worker thread.
func (wq *WorkQueue) Retry(task *Task) error {
	wq.mu.Lock()

	if task.Retries >= task.MaxRetries {
		wq.mu.Unlock()
		wq.Done(task)
		return errors.New("max retries exceeded")
	}

	task.Retries++
	task.LastFailure = time.Now()

	// Apply exponential backoff (100ms * 2^retries) to stagger retries.
	backoffDuration := time.Duration(100<<task.Retries) * time.Millisecond
	wq.mu.Unlock()

	// Asynchronously wait for the backoff duration before re-enqueuing the task.
	go func(t *Task, d time.Duration) {
		time.Sleep(d)

		wq.mu.Lock()
		defer wq.mu.Unlock()

		if wq.shuttingDown {
			return
		}

		delete(wq.processing, t.NodeID)
		wq.dirty[t.NodeID] = t
		wq.queue = append(wq.queue, t)
		wq.cond.Signal()
	}(task, backoffDuration)

	return nil
}

// ShutDown transitions the queue to a terminal state, preventing new task additions
// and unblocking any pending Get() calls.
func (wq *WorkQueue) ShutDown() {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	wq.shuttingDown = true
	wq.cond.Broadcast()
}

// Length returns the current number of items waiting in the queue.
func (wq *WorkQueue) Length() int {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	return len(wq.queue)
}
