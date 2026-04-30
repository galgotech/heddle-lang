package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func TestWorkQueue_AddGetDone(t *testing.T) {
	wq := NewWorkQueue(rate.Inf, 1, nil)
	defer wq.ShutDown()

	wq.Add("node-1", 3)
	assert.Equal(t, 1, wq.Length())

	task, shuttingDown := wq.Get()
	assert.False(t, shuttingDown)
	assert.Equal(t, "node-1", task.NodeID)
	assert.Equal(t, 0, wq.Length()) // Task is in processing

	// Adding while processing should mark it as dirty but not add to queue
	wq.Add("node-1", 3)
	assert.Equal(t, 0, wq.Length())

	// Marking as done should re-queue the dirty task
	wq.Done(task)
	assert.Equal(t, 1, wq.Length())

	task2, _ := wq.Get()
	assert.Equal(t, "node-1", task2.NodeID)
	wq.Done(task2)
	assert.Equal(t, 0, wq.Length())
}

func TestWorkQueue_RateLimiting(t *testing.T) {
	// 10 qps, burst 1 -> 100ms per task
	wq := NewWorkQueue(10, 1, nil)
	defer wq.ShutDown()

	ctx := context.Background()

	start := time.Now()

	// First one should pass immediately
	err := wq.AddRateLimited(ctx, "node-1", 3)
	assert.NoError(t, err)

	// Second one should take ~100ms
	err = wq.AddRateLimited(ctx, "node-2", 3)
	assert.NoError(t, err)

	duration := time.Since(start)
	assert.True(t, duration >= 90*time.Millisecond, "Rate limit should have delayed the second add")
	assert.Equal(t, 2, wq.Length())
}

func TestWorkQueue_RetryBackoff(t *testing.T) {
	wq := NewWorkQueue(rate.Inf, 1, nil)
	defer wq.ShutDown()

	wq.Add("node-1", 2)

	task, _ := wq.Get()

	err := wq.Retry(task)
	assert.NoError(t, err)
	assert.Equal(t, 1, task.Retries)

	// Wait for the re-queue (should take ~200ms)
	time.Sleep(250 * time.Millisecond)
	assert.Equal(t, 1, wq.Length(), "Task should have been re-queued after backoff")

	requeuedTask, _ := wq.Get()
	assert.Equal(t, "node-1", requeuedTask.NodeID)

	// Retry again
	err = wq.Retry(requeuedTask)
	assert.NoError(t, err)
	assert.Equal(t, 2, requeuedTask.Retries)

	// Wait for the re-queue (should take ~400ms)
	time.Sleep(450 * time.Millisecond)
	requeuedTask2, _ := wq.Get()

	// Third retry should fail (max retries = 2)
	err = wq.Retry(requeuedTask2)
	assert.Error(t, err)
	assert.Equal(t, "max retries exceeded", err.Error())
	assert.Equal(t, 0, wq.Length(), "Task should be dropped after max retries")
}

func TestWorkQueue_ShutDown(t *testing.T) {
	wq := NewWorkQueue(rate.Inf, 1, nil)

	wq.Add("node-1", 3)
	wq.ShutDown()

	// Should still be able to get existing items
	task, shut := wq.Get()
	assert.False(t, shut)
	assert.Equal(t, "node-1", task.NodeID)

	// Next get should indicate shutdown
	_, shut = wq.Get()
	assert.True(t, shut)

	// Should not be able to add new items
	wq.Add("node-2", 3)
	assert.Equal(t, 0, wq.Length())
}

func TestWorkQueue_Concurrency(t *testing.T) {
	wq := NewWorkQueue(rate.Inf, 1, nil)
	defer wq.ShutDown()

	var wg sync.WaitGroup
	numWorkers := 10
	numTasks := 100

	// Add tasks concurrently
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numTasks/numWorkers; j++ {
				// Same node IDs might overlap, simulating complex DAG routing
				wq.Add("node-concurrent", 3)
			}
		}(i)
	}

	wg.Wait()

	// Consume tasks concurrently
	var consumeWg sync.WaitGroup
	consumeWg.Add(numWorkers)

	processed := 0
	var mu sync.Mutex

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer consumeWg.Done()
			for {
				task, shut := wq.Get()
				if shut {
					return
				}

				mu.Lock()
				processed++
				mu.Unlock()

				wq.Done(task)

				mu.Lock()
				if processed == 1 { // We only expect 1 because all tasks had the same ID "node-concurrent"
					wq.ShutDown() // Signal others to stop
				}
				mu.Unlock()
			}
		}()
	}

	consumeWg.Wait()
	assert.Equal(t, 1, processed, "Should only process 1 task because they all had the same ID and were deduplicated")
}
