package control_plane

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/galgotech/heddle-lang/internal/services/models"
)

func TestTaskQueue_Concurrency(t *testing.T) {
	q := NewTaskQueue()

	count := 100
	for i := 0; i < count; i++ {
		q.Push(models.Task{ID: "task-id"})
	}

	assert.Equal(t, count, q.Len())

	for i := 0; i < count; i++ {
		task := q.Pop()
		assert.Equal(t, "task-id", task.ID)
	}

	assert.Equal(t, 0, q.Len())
}

func TestTaskQueue_Blocking(t *testing.T) {
	q := NewTaskQueue()

	go func() {
		time.Sleep(100 * time.Millisecond)
		q.Push(models.Task{ID: "delayed-task"})
	}()

	start := time.Now()
	task := q.Pop()
	elapsed := time.Since(start)

	assert.Equal(t, "delayed-task", task.ID)
	assert.True(t, elapsed >= 100*time.Millisecond)
}
