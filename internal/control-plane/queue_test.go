package control_plane

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/galgotech/heddle-lang/internal/models"
)

func TestTaskQueue_FIFO(t *testing.T) {
	q := NewTaskQueue()

	assert.Equal(t, 0, q.Len())

	q.Push(models.Task{ID: "task-1"})
	q.Push(models.Task{ID: "task-2"})

	assert.Equal(t, 2, q.Len())

	ch := q.Pop()

	task1 := <-ch
	assert.Equal(t, "task-1", task1.ID)
	assert.Equal(t, 1, q.Len())

	task2 := <-ch
	assert.Equal(t, "task-2", task2.ID)
	assert.Equal(t, 0, q.Len())
}

func TestTaskQueue_Concurrency(t *testing.T) {
	q := NewTaskQueue()

	numWorkers := 10
	numTasksPerWorker := 100
	var wg sync.WaitGroup

	// Start concurrent pushers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numTasksPerWorker; j++ {
				q.Push(models.Task{ID: "task"})
			}
		}(i)
	}

	wg.Wait()

	totalExpected := numWorkers * numTasksPerWorker
	assert.Equal(t, totalExpected, q.Len())

	// Start concurrent poppers
	ch := q.Pop()
	var count int
	var countMu sync.Mutex

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numTasksPerWorker; j++ {
				<-ch
				countMu.Lock()
				count++
				countMu.Unlock()
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, totalExpected, count)
	assert.Equal(t, 0, q.Len())
}

func TestTaskQueue_Blocking(t *testing.T) {
	q := NewTaskQueue()

	ch := q.Pop()

	// Verify it blocks until a task is available
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	select {
	case <-ch:
		t.Fatal("expected reading from Pop channel to block on empty queue")
	case <-ctx.Done():
		// Blocked as expected
	}

	// Now push a task asynchronously
	go func() {
		time.Sleep(50 * time.Millisecond)
		q.Push(models.Task{ID: "delayed-task"})
	}()

	start := time.Now()
	task := <-ch
	elapsed := time.Since(start)

	assert.Equal(t, "delayed-task", task.ID)
	assert.True(t, elapsed >= 50*time.Millisecond)
}
