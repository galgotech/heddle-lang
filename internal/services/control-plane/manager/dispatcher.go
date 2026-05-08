package manager

import (
	"context"

	"github.com/galgotech/heddle-lang/internal/services/control-plane/scheduler"
	"github.com/galgotech/heddle-lang/internal/services/control-plane/state"
)

// ExecutionFunc specifies the signature for the low-level task execution bridge to the worker fleet.
type ExecutionFunc func(ctx context.Context, workerID string, nodeID string) error

// Dispatcher manages the consumption of tasks from the scheduler and their delegation to the worker fleet.
// It acts as the bridge between logical DAG ordering and physical execution.
type Dispatcher interface {
	// Start begins the task processing loops with the specified concurrency level.
	Start(concurrency int)
	// Stop gracefully shuts down the dispatcher and waits for active loops to terminate.
	Stop()
	// Dispatch selects a healthy worker and executes a single task.
	Dispatch(ctx context.Context, task *scheduler.Task) error
}

// DefaultDispatcher implements the Dispatcher interface, leveraging a workqueue and a concurrency pool.
type DefaultDispatcher struct {
	queue    *scheduler.WorkQueue
	registry WorkerRegistry
	sm       state.StateMachine
	executor ExecutionFunc
	pool     ConcurrencyPool
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewDispatcher initializes a dispatcher with its required orchestration and state management dependencies.
func NewDispatcher(queue *scheduler.WorkQueue, registry WorkerRegistry, sm state.StateMachine, executor ExecutionFunc, pool ConcurrencyPool) Dispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &DefaultDispatcher{
		queue:    queue,
		registry: registry,
		sm:       sm,
		executor: executor,
		pool:     pool,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start spawns the configured number of concurrent worker loops to process tasks from the scheduler.
func (d *DefaultDispatcher) Start(concurrency int) {
	for range concurrency {
		d.pool.Go(d.workerLoop)
	}
}

// Stop initiates a graceful shutdown by cancelling the context and draining the workqueue.
func (d *DefaultDispatcher) Stop() {
	d.cancel()
	d.queue.ShutDown()
	d.pool.Wait()
}

// Dispatch selects an optimal worker and executes a task. It performs pre-flight
// context checks to ensure the operation hasn't been cancelled before reaching the worker.
func (d *DefaultDispatcher) Dispatch(ctx context.Context, task *scheduler.Task) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Retrieve a healthy node from the registry for task delegation.
	worker, err := d.registry.GetHealthyWorker()
	if err != nil {
		return err
	}

	// Trigger the execution via the configured bridge (e.g., gRPC call).
	return d.executor(ctx, worker.ID, task.NodeID)
}

// workerLoop runs the continuous fetch-dispatch-state cycle for task execution.
// It ensures fault tolerance by leveraging the scheduler's retry and backoff mechanisms.
func (d *DefaultDispatcher) workerLoop() {
	for {
		// Block until a task is available or the scheduler is shutting down.
		task, shuttingDown := d.queue.Get()
		if shuttingDown {
			return
		}

		// Atomically transition the task state to Running to prevent duplicate executions across loops.
		_ = d.sm.Transition(task.NodeID, state.Pending, state.Running, nil)

		err := d.Dispatch(d.ctx, task)
		if err != nil {
			// Evaluate the retry budget to determine if the node should return to Pending or transition to Failed.
			if task.Retries < task.MaxRetries {
				_ = d.sm.Transition(task.NodeID, state.Running, state.Pending, err)
			} else {
				_ = d.sm.Transition(task.NodeID, state.Running, state.Failed, err)
			}
			// Re-enqueue the task for deferred retry according to the scheduler's backoff strategy.
			d.queue.Retry(task)
		} else {
			// Mark the node as successfully completed in the global state machine.
			_ = d.sm.Transition(task.NodeID, state.Running, state.Completed, nil)
			// Signal successful completion to the scheduler to unblock downstream dependencies in the DAG.
			d.queue.Done(task)
		}
	}
}
