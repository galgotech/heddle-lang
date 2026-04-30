package manager

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/galgotech/heddle-lang/services/control-plane/pkg/heddle/scheduler"
)

// WorkerState represents the health state of a worker.
type WorkerState string

const (
	WorkerHealthy WorkerState = "Healthy"
	WorkerDegraded WorkerState = "Degraded"
	WorkerOffline WorkerState = "Offline"
)

// Worker represents a "Dumb Worker" (e.g., Node.js, Python, Rust) executing DataFusion logic.
type Worker struct {
	ID         string
	Labels     map[string]string
	State      WorkerState
	LastSeenAt time.Time
}

// Registry manages the registration and health of dumb workers.
type Registry struct {
	mu      sync.RWMutex
	workers map[string]*Worker
}

// NewRegistry creates a new worker registry.
func NewRegistry() *Registry {
	return &Registry{
		workers: make(map[string]*Worker),
	}
}

// Register adds or updates a worker's health status.
func (r *Registry) Register(id string, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.workers[id] = &Worker{
		ID:         id,
		Labels:     labels,
		State:      WorkerHealthy,
		LastSeenAt: time.Now(),
	}
}

// Heartbeat updates the LastSeenAt timestamp for a worker.
func (r *Registry) Heartbeat(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	worker, exists := r.workers[id]
	if !exists {
		return errors.New("worker not found")
	}

	worker.LastSeenAt = time.Now()
	worker.State = WorkerHealthy
	return nil
}

// GetHealthyWorker returns a random healthy worker (placeholder for round-robin/least-conn).
func (r *Registry) GetHealthyWorker() (*Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, w := range r.workers {
		// Consider offline if unseen for > 30s
		if time.Since(w.LastSeenAt) > 30*time.Second {
			continue
		}
		if w.State == WorkerHealthy {
			return w, nil
		}
	}

	return nil, errors.New("no healthy workers available")
}

// ExecutionFunc is a mock signature for actual gRPC DataFusion execution.
type ExecutionFunc func(ctx context.Context, workerID string, nodeID string) error

// Dispatcher bridges the workqueue and the dumb workers.
type Dispatcher struct {
	queue    *scheduler.WorkQueue
	registry *Registry
	executor ExecutionFunc
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewDispatcher creates a new task dispatcher.
func NewDispatcher(queue *scheduler.WorkQueue, registry *Registry, executor ExecutionFunc) *Dispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Dispatcher{
		queue:    queue,
		registry: registry,
		executor: executor,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start spawns goroutines to pull from the workqueue and execute tasks.
func (d *Dispatcher) Start(concurrency int) {
	for i := 0; i < concurrency; i++ {
		d.wg.Add(1)
		go d.workerLoop()
	}
}

// Stop cleanly shuts down the dispatcher.
func (d *Dispatcher) Stop() {
	d.cancel()
	d.queue.ShutDown()
	d.wg.Wait()
}

func (d *Dispatcher) workerLoop() {
	defer d.wg.Done()

	for {
		task, shuttingDown := d.queue.Get()
		if shuttingDown {
			return
		}

		select {
		case <-d.ctx.Done():
			d.queue.Done(task) // Put it back/mark done
			return
		default:
		}

		// Find a healthy worker
		worker, err := d.registry.GetHealthyWorker()
		if err != nil {
			// No workers? Retry with backoff
			d.queue.Retry(task)
			continue
		}

		// Execute the stateful task deterministically
		err = d.executor(d.ctx, worker.ID, task.NodeID)
		if err != nil {
			// Failed execution, retry
			d.queue.Retry(task)
		} else {
			// Success
			d.queue.Done(task)
		}
	}
}
