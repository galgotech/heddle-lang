package control_plane

import (
	"github.com/galgotech/heddle-lang/internal/models"
)

type TaskQueue struct {
	buffer chan models.Task
}

func (q *TaskQueue) Push(task models.Task) {
	q.buffer <- task
}

func (q *TaskQueue) Pop() <-chan models.Task {
	return q.buffer
}

func (q *TaskQueue) Len() int {
	return len(q.buffer)
}

func NewTaskQueue() *TaskQueue {
	q := &TaskQueue{
		buffer: make(chan models.Task, 1000),
	}
	return q
}
