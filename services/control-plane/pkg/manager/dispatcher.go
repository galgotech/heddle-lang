package manager

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/galgotech/heddle-lang/services/control-plane/pkg/scheduler"
)

// WorkerState defines the operational health status of a remote execution node.
type WorkerState string

const (
	// WorkerHealthy indicates the worker is actively responding to heartbeats and available for task assignment.
	WorkerHealthy WorkerState = "Healthy"
	// WorkerDegraded indicates the worker is reachable but experiencing performance issues or resource exhaustion.
	WorkerDegraded WorkerState = "Degraded"
	// WorkerOffline indicates the worker has missed multiple heartbeats or explicitly disconnected.
	WorkerOffline WorkerState = "Offline"
)

// Worker encapsulates the metadata and state of a stateless execution unit ("Dumb Worker").
// In the Heddle architecture, these workers receive dynamic JIT code injections from the
// Control Plane to execute declarative flow controls over Arrow-native data.
type Worker struct {
	ID         string            // Unique identifier for the worker node.
	Address    string            // Network address (TCP/gRPC) for remote task delegation.
	UDSAddress string            // Unix Domain Socket path for high-performance, zero-copy local communication.
	Labels     map[string]string // Metadata for capability-based scheduling (e.g., hardware accelerators, regions).
	State      WorkerState       // Current operational health status.
	LastSeenAt time.Time         // Timestamp of the most recently received heartbeat or registration.
}

// Registry provides a thread-safe repository for tracking and managing the lifecycle
// of all active workers within the cluster.
type Registry struct {
	mu      sync.RWMutex
	workers map[string]*Worker
}

// NewRegistry initializes an empty worker registry.
func NewRegistry() *Registry {
	return &Registry{
		workers: make(map[string]*Worker),
	}
}

// Register adds or updates a worker's registration in the registry.
// This is invoked during worker bootstrap or when updating static metadata like labels or addresses.
func (r *Registry) Register(id string, address string, udsAddress string, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[id] = &Worker{
		ID:         id,
		Address:    address,
		UDSAddress: udsAddress,
		Labels:     labels,
		State:      WorkerHealthy,
		LastSeenAt: time.Now(),
	}
}

// GetWorker retrieves a specific worker by its unique identifier.
func (r *Registry) GetWorker(id string) (*Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	worker, exists := r.workers[id]
	if !exists {
		return nil, errors.New("worker not found")
	}
	return worker, nil
}

// Heartbeat refreshes the liveness timestamp and health status for a worker.
// This maintains the worker's eligibility for task assignment in the dispatcher loop.
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

// GetHealthyWorker selects an available worker for task assignment.
// Currently implements a basic selection strategy with a 30-second liveness TTL.
func (r *Registry) GetHealthyWorker() (*Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, w := range r.workers {
		// Enforce a strict 30-second window before considering a worker unavailable.
		if time.Since(w.LastSeenAt) > 30*time.Second {
			continue
		}
		if w.State == WorkerHealthy {
			return w, nil
		}
	}

	return nil, errors.New("no healthy workers available")
}

// ExecutionFunc defines the functional contract for dispatching tasks to workers.
// It abstracts the underlying transport (e.g., gRPC, Arrow Flight) from the orchestration logic.
type ExecutionFunc func(ctx context.Context, workerID string, nodeID string) error

// Dispatcher bridges the logical DAG scheduler (WorkQueue) and the physical execution fleet (Workers).
// It implements the consumer loop that pulls tasks and delegates them to available workers.
type Dispatcher struct {
	queue    *scheduler.WorkQueue
	registry *Registry
	executor ExecutionFunc
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewDispatcher initializes a dispatcher with its required orchestration dependencies.
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

// Start spawns the configured number of parallel worker loops to process tasks from the queue.
func (d *Dispatcher) Start(concurrency int) {
	for range concurrency {
		d.wg.Add(1)
		go d.workerLoop()
	}
}

// Stop initiates a graceful shutdown of the dispatcher, draining active loops and closing the queue.
func (d *Dispatcher) Stop() {
	d.cancel()
	d.queue.ShutDown()
	d.wg.Wait()
}

// workerLoop executes the primary task orchestration lifecycle: pull, locate worker, and execute.
// It ensures fault tolerance by leveraging the workqueue's retry and backoff mechanisms.
func (d *Dispatcher) workerLoop() {
	defer d.wg.Done()

	for {
		// Block until a task is available or the system is shutting down.
		task, shuttingDown := d.queue.Get()
		if shuttingDown {
			return
		}

		// Verify context liveness before proceeding with worker selection.
		select {
		case <-d.ctx.Done():
			d.queue.Done(task) // Ensure task is marked as finished if shutdown occurs mid-loop.
			return
		default:
		}

		// Locate a healthy worker node for this task.
		worker, err := d.registry.GetHealthyWorker()
		if err != nil {
			// Requeue the task for future dispatch if no workers are currently available.
			d.queue.Retry(task)
			continue
		}

		// Execute the task on the remote worker node.
		err = d.executor(d.ctx, worker.ID, task.NodeID)
		if err != nil {
			// Trigger a retry on execution failure, adhering to the scheduler's backoff strategy.
			d.queue.Retry(task)
		} else {
			// Signal successful completion to the workqueue.
			d.queue.Done(task)
		}
	}
}
