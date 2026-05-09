package controlplane

import (
	"sync"
	"testing"
	"time"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/stretchr/testify/assert"
)

func TestWorkQueue_AddGetDone(t *testing.T) {
	wq := NewWorkQueue()
	defer wq.ShutDown()

	p1 := &ir.Program{Instructions: map[string]any{"node-1": nil}}
	wq.Add(p1, 3)
	assert.Equal(t, 1, wq.Length())

	task, shuttingDown := wq.Get()
	assert.False(t, shuttingDown)
	assert.Equal(t, p1, task.Program)
	assert.Equal(t, 0, wq.Length()) // Task is in processing

	// Adding while processing should mark it as dirty but not add to queue
	wq.Add(p1, 3)
	assert.Equal(t, 0, wq.Length())

	// Marking as done should re-queue the dirty task
	wq.Done(task)
	assert.Equal(t, 1, wq.Length())

	task2, _ := wq.Get()
	assert.Equal(t, p1, task2.Program)
	wq.Done(task2)
	assert.Equal(t, 0, wq.Length())
}

func TestWorkQueue_RetryBackoff(t *testing.T) {
	wq := NewWorkQueue()
	defer wq.ShutDown()

	p1 := &ir.Program{Instructions: map[string]any{"node-1": nil}}
	wq.Add(p1, 2)

	task, _ := wq.Get()

	err := wq.Retry(task)
	assert.NoError(t, err)
	assert.Equal(t, 1, task.Retries)

	// Wait for the re-queue (should take ~200ms)
	time.Sleep(250 * time.Millisecond)
	assert.Equal(t, 1, wq.Length(), "Task should have been re-queued after backoff")

	requeuedTask, _ := wq.Get()
	assert.Equal(t, p1, requeuedTask.Program)

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
	wq := NewWorkQueue()

	p1 := &ir.Program{Instructions: map[string]any{"node-1": nil}}
	wq.Add(p1, 3)
	wq.ShutDown()

	// Should still be able to get existing items
	task, shut := wq.Get()
	assert.False(t, shut)
	assert.Equal(t, p1, task.Program)

	// Next get should indicate shutdown
	_, shut = wq.Get()
	assert.True(t, shut)

	// Should not be able to add new items
	p2 := &ir.Program{Instructions: map[string]any{"node-2": nil}}
	wq.Add(p2, 3)
	assert.Equal(t, 0, wq.Length())
}

func TestWorkQueue_Concurrency(t *testing.T) {
	wq := NewWorkQueue()
	defer wq.ShutDown()

	var wg sync.WaitGroup
	numWorkers := 10
	numTasks := 100

	p1 := &ir.Program{Instructions: map[string]any{"node-concurrent": nil}}

	// Add tasks concurrently
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numTasks/numWorkers; j++ {
				// Same node IDs might overlap, simulating complex DAG routing
				wq.Add(p1, 3)
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
