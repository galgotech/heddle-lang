package control_plane

import (
	"github.com/galgotech/heddle-lang/internal/services/models"
)

type TaskQueue struct {
	pushCh chan models.Task
	popCh  chan models.Task
	lenCh  chan chan int
}

func (q *TaskQueue) run() {
	var queue []models.Task
	for {
		var first models.Task
		var out chan models.Task

		if len(queue) > 0 {
			first = queue[0]
			out = q.popCh
		}

		select {
		case task := <-q.pushCh:
			queue = append(queue, task)
		case out <- first:
			queue = queue[1:]
		case reply := <-q.lenCh:
			reply <- len(queue)
		}
	}
}

func (q *TaskQueue) Push(task models.Task) {
	q.pushCh <- task
}

func (q *TaskQueue) Pop() models.Task {
	return <-q.popCh
}

func (q *TaskQueue) Len() int {
	reply := make(chan int)
	q.lenCh <- reply
	return <-reply
}

func NewTaskQueue() *TaskQueue {
	q := &TaskQueue{
		pushCh: make(chan models.Task),
		popCh:  make(chan models.Task),
		lenCh:  make(chan chan int),
	}
	go q.run()
	return q
}
