package controlplane

import (
	"errors"
	"time"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
)

// Task represents a unit of work within the scheduler.
type Task struct {
	Program     *ir.Program
	Retries     int
	MaxRetries  int
	LastFailure time.Time
}

type addTaskRequest struct {
	program    *ir.Program
	maxRetries int
	respCh     chan struct{}
}

type getTaskRequest struct {
	respCh chan getTaskResponse
}

type getTaskResponse struct {
	task         *Task
	shuttingDown bool
}

type markTaskDoneRequest struct {
	task   *Task
	respCh chan struct{}
}

type retryTaskRequest struct {
	task   *Task
	respCh chan error
}

type getQueueLengthRequest struct {
	respCh chan int
}

type reenqueueTaskRequest struct {
	task *Task
}

// WorkQueue implements a lock-free task distribution system using a manager goroutine.
type WorkQueue struct {
	addTaskCh        chan addTaskRequest
	getTaskCh        chan getTaskRequest
	markTaskDoneCh   chan markTaskDoneRequest
	retryTaskCh      chan retryTaskRequest
	getQueueLengthCh chan getQueueLengthRequest
	reenqueueTaskCh  chan reenqueueTaskRequest
	shutdownCh       chan struct{}
}

func (wq *WorkQueue) Add(program *ir.Program, maxRetries int) {
	respCh := make(chan struct{}, 1)
	wq.addTaskCh <- addTaskRequest{program, maxRetries, respCh}
	<-respCh
}

func (wq *WorkQueue) Get() (*Task, bool) {
	respCh := make(chan getTaskResponse, 1)
	wq.getTaskCh <- getTaskRequest{respCh}
	resp := <-respCh
	return resp.task, resp.shuttingDown
}

func (wq *WorkQueue) Done(task *Task) {
	respCh := make(chan struct{}, 1)
	wq.markTaskDoneCh <- markTaskDoneRequest{task, respCh}
	<-respCh
}

func (wq *WorkQueue) Retry(task *Task) error {
	respCh := make(chan error, 1)
	wq.retryTaskCh <- retryTaskRequest{task, respCh}
	return <-respCh
}

func (wq *WorkQueue) ShutDown() {
	select {
	case <-wq.shutdownCh:
	default:
		close(wq.shutdownCh)
	}
}

func (wq *WorkQueue) Length() int {
	respCh := make(chan int, 1)
	wq.getQueueLengthCh <- getQueueLengthRequest{respCh}
	return <-respCh
}

func (wq *WorkQueue) run() {
	queue := make([]*Task, 0)
	processing := make(map[string]*Task)
	dirty := make(map[string]*Task)
	shuttingDown := false

	var pendingGets []chan getTaskResponse

	for {
		// Fulfill pending gets if possible
		if len(queue) > 0 && len(pendingGets) > 0 {
			task := queue[0]
			queue = queue[1:]
			id := task.Program.GetID()
			processing[id] = task
			delete(dirty, id)

			pendingGets[0] <- getTaskResponse{task, false}
			pendingGets = pendingGets[1:]
			continue
		}

		select {
		case req := <-wq.addTaskCh:
			if shuttingDown {
				req.respCh <- struct{}{}
				continue
			}
			id := req.program.GetID()
			if _, exists := dirty[id]; exists {
				req.respCh <- struct{}{}
				continue
			}
			task := &Task{
				Program:    req.program,
				MaxRetries: req.maxRetries,
			}
			dirty[id] = task
			if _, proc := processing[id]; !proc {
				queue = append(queue, task)
			}
			req.respCh <- struct{}{}

		case req := <-wq.getTaskCh:
			if len(queue) > 0 {
				task := queue[0]
				queue = queue[1:]
				id := task.Program.GetID()
				processing[id] = task
				delete(dirty, id)
				req.respCh <- getTaskResponse{task, false}
			} else if shuttingDown {
				req.respCh <- getTaskResponse{nil, true}
			} else {
				pendingGets = append(pendingGets, req.respCh)
			}

		case req := <-wq.markTaskDoneCh:
			id := req.task.Program.GetID()
			delete(processing, id)
			if dirtyTask, exists := dirty[id]; exists {
				queue = append(queue, dirtyTask)
			}
			req.respCh <- struct{}{}

		case req := <-wq.retryTaskCh:
			if req.task.Retries >= req.task.MaxRetries {
				req.respCh <- errors.New("max retries exceeded")
				id := req.task.Program.GetID()
				delete(processing, id)
				continue
			}
			req.task.Retries++
			req.task.LastFailure = time.Now()
			backoff := time.Duration(100<<req.task.Retries) * time.Millisecond

			id := req.task.Program.GetID()
			delete(processing, id)
			delete(dirty, id)

			go func(t *Task, d time.Duration) {
				time.Sleep(d)
				wq.reenqueueTaskCh <- reenqueueTaskRequest{t}
			}(req.task, backoff)

			req.respCh <- nil

		case req := <-wq.reenqueueTaskCh:
			if shuttingDown {
				continue
			}
			id := req.task.Program.GetID()
			dirty[id] = req.task
			if _, proc := processing[id]; !proc {
				queue = append(queue, req.task)
			}

		case req := <-wq.getQueueLengthCh:
			req.respCh <- len(queue)

		case <-wq.shutdownCh:
			if shuttingDown {
				continue
			}
			shuttingDown = true
			for _, ch := range pendingGets {
				ch <- getTaskResponse{nil, true}
			}
			pendingGets = nil
		}
	}
}

func NewWorkQueue() *WorkQueue {
	wq := &WorkQueue{
		addTaskCh:        make(chan addTaskRequest, 100),
		getTaskCh:        make(chan getTaskRequest, 10),
		markTaskDoneCh:   make(chan markTaskDoneRequest, 100),
		retryTaskCh:      make(chan retryTaskRequest, 10),
		getQueueLengthCh: make(chan getQueueLengthRequest, 10),
		reenqueueTaskCh:  make(chan reenqueueTaskRequest, 10),
		shutdownCh:       make(chan struct{}),
	}
	go wq.run()
	return wq
}
