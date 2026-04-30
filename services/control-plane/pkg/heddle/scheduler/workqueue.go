package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Task represents a unit of work in the queue.
type Task struct {
	NodeID      string
	Retries     int
	MaxRetries  int
	LastFailure time.Time
}

// AoTAnalyzer is an interface to inspect workload before scheduling.
type AoTAnalyzer interface {
	Analyze(task *Task) (priority int, group string)
}

// DefaultAnalyzer is a simple analyzer that treats all tasks equally.
type DefaultAnalyzer struct{}

func (d *DefaultAnalyzer) Analyze(task *Task) (int, string) {
	return 0, "default"
}

// WorkQueue is a highly concurrent, k8s-inspired task distribution queue.
type WorkQueue struct {
	mu           sync.Mutex
	cond         *sync.Cond
	queue        []*Task
	processing   map[string]*Task
	dirty        map[string]*Task

	rateLimiter  *rate.Limiter
	analyzer     AoTAnalyzer

	taskPool     sync.Pool

	shuttingDown bool
}

// NewWorkQueue creates a new workqueue with token-bucket rate limiting.
func NewWorkQueue(qps rate.Limit, burst int, analyzer AoTAnalyzer) *WorkQueue {
	if analyzer == nil {
		analyzer = &DefaultAnalyzer{}
	}

	wq := &WorkQueue{
		queue:      make([]*Task, 0),
		processing: make(map[string]*Task),
		dirty:      make(map[string]*Task),
		rateLimiter: rate.NewLimiter(qps, burst),
		analyzer:   analyzer,
		taskPool: sync.Pool{
			New: func() interface{} {
				return &Task{}
			},
		},
	}
	wq.cond = sync.NewCond(&wq.mu)
	return wq
}

// Add enqueues a new task.
func (wq *WorkQueue) Add(nodeID string, maxRetries int) {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.shuttingDown {
		return
	}

	if _, exists := wq.dirty[nodeID]; exists {
		// Already added but not processed yet
		return
	}

	// Task allocation via sync.Pool to reduce GC
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

// AddRateLimited adds an item after checking the rate limiter.
func (wq *WorkQueue) AddRateLimited(ctx context.Context, nodeID string, maxRetries int) error {
	if err := wq.rateLimiter.Wait(ctx); err != nil {
		return err
	}
	wq.Add(nodeID, maxRetries)
	return nil
}

// Get blocks until a task is available to process.
func (wq *WorkQueue) Get() (*Task, bool) {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	for len(wq.queue) == 0 && !wq.shuttingDown {
		wq.cond.Wait()
	}

	if len(wq.queue) == 0 {
		return nil, true // Shutting down
	}

	task := wq.queue[0]
	wq.queue = wq.queue[1:]

	wq.processing[task.NodeID] = task
	delete(wq.dirty, task.NodeID)

	return task, false
}

// Done marks a task as finished and cleans it up.
func (wq *WorkQueue) Done(task *Task) {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	delete(wq.processing, task.NodeID)

	// If it was added again while processing, re-queue it
	if dirtyTask, exists := wq.dirty[task.NodeID]; exists {
		wq.queue = append(wq.queue, dirtyTask)
		wq.cond.Signal()
	} else {
		// Otherwise return to pool
		task.NodeID = ""
		wq.taskPool.Put(task)
	}
}

// Retry schedules a failed task for retry with exponential backoff if possible.
func (wq *WorkQueue) Retry(task *Task) error {
	wq.mu.Lock()

	if task.Retries >= task.MaxRetries {
		wq.mu.Unlock()
		wq.Done(task)
		return errors.New("max retries exceeded")
	}

	task.Retries++
	task.LastFailure = time.Now()

	// Calculate backoff: e.g., 100ms * 2^retries
	backoffDuration := time.Duration(100<<task.Retries) * time.Millisecond
	wq.mu.Unlock()

	// Wait before adding back (non-blocking in caller by using a goroutine)
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

// ShutDown stops the queue from accepting new tasks.
func (wq *WorkQueue) ShutDown() {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	wq.shuttingDown = true
	wq.cond.Broadcast()
}

// Length returns the current length of the queue.
func (wq *WorkQueue) Length() int {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	return len(wq.queue)
}
