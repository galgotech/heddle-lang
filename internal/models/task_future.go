package models

import (
	"context"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

// TaskFuture provides a safe, buffered channel wrapper for asynchronous task execution results
type TaskFuture struct {
	ch chan TaskResult
}

// NewTaskFuture creates a new TaskFuture.
// The internal channel is guaranteed to have a buffer size of 1,
// preventing head-of-line blocking deadlocks in the FlightRPC stream router.
func NewTaskFuture() *TaskFuture {
	return &TaskFuture{
		ch: make(chan TaskResult, 1),
	}
}

// Resolve pushes a TaskResult into the future.
// It returns true if successful, or false if the future was already resolved or full.
func (f *TaskFuture) Resolve(res TaskResult) bool {
	select {
	case f.ch <- res:
		return true
	default:
		logger.L().Error("TaskFuture is full, the received more then one response for the same task")
		return false
	}
}

// Await blocks until the future is resolved, or the context is canceled.
func (f *TaskFuture) Await(ctx context.Context) (TaskResult, error) {
	select {
	case res := <-f.ch:
		return res, nil
	case <-ctx.Done():
		return TaskResult{}, ctx.Err()
	}
}

// Close explicitly closes the internal channel. Useful during shutdown.
func (f *TaskFuture) Close() {
	close(f.ch)
}
