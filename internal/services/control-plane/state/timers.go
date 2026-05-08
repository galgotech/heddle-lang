package state

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// TimerTask defines a scheduled execution event for a DAG node.
// Implemented as a pointerless struct to minimize heap fragmentation and GC scanning overhead.
type TimerTask struct {
	NodeID   string
	FireTime time.Time
}

// timerHeap provides a min-priority queue implementation for TimerTasks,
// ordered by their firing timestamp.
type timerHeap []TimerTask

func (h timerHeap) Len() int {
	return len(h)
}

func (h timerHeap) Less(i, j int) bool {
	return h[i].FireTime.Before(h[j].FireTime)
}

func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *timerHeap) Push(x any) {
	*h = append(*h, x.(TimerTask))
}

func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Peek returns the earliest task from the heap without modification.
// Returns nil if the heap is empty.
func (h *timerHeap) Peek() *TimerTask {
	if len(*h) == 0 {
		return nil
	}
	return &(*h)[0]
}

// TimerManager orchestrates massively concurrent wait-states for the control plane.
// It multiplexes multiple logical timers into a single background runner loop using
// a min-priority queue, ensuring high efficiency for thousands of concurrent nodes.
type TimerManager struct {
	mu       sync.Mutex
	tasks    timerHeap
	wakeup   chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	callback func(nodeID string)
	wg       sync.WaitGroup
}

// NewTimerManager initializes the manager and starts the background orchestration loop.
func NewTimerManager(callback func(nodeID string)) *TimerManager {
	ctx, cancel := context.WithCancel(context.Background())
	tm := &TimerManager{
		tasks:    make(timerHeap, 0),
		wakeup:   make(chan struct{}, 1),
		ctx:      ctx,
		cancel:   cancel,
		callback: callback,
	}
	heap.Init(&tm.tasks)
	tm.wg.Add(1)
	go tm.run()
	return tm
}

// Schedule registers a task for future execution.
// If the new task fires earlier than the current queue head, it triggers a
// runner wakeup to recalibrate the wait interval.
func (tm *TimerManager) Schedule(nodeID string, delay time.Duration) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	task := TimerTask{
		NodeID:   nodeID,
		FireTime: time.Now().Add(delay),
	}
	heap.Push(&tm.tasks, task)

	// Notify the runner to re-evaluate the next firing window.
	select {
	case tm.wakeup <- struct{}{}:
	default:
	}
}

// run executes the primary timing loop.
// It dynamically adjusts sleep intervals based on the priority queue and executes
// ready tasks in separate goroutines to prevent callback latency from drifting the schedule.
func (tm *TimerManager) run() {
	defer tm.wg.Done()
	timer := time.NewTimer(0)
	<-timer.C // Ensure the timer is drained before the first use.

	for {
		tm.mu.Lock()
		nextTask := tm.tasks.Peek()
		var delay time.Duration
		hasTask := false

		if nextTask != nil {
			now := time.Now()
			if now.After(nextTask.FireTime) || now.Equal(nextTask.FireTime) {
				// Task is ready; dequeue and dispatch.
				task := heap.Pop(&tm.tasks).(TimerTask)
				tm.mu.Unlock()

				// Asynchronous dispatch preserves timing precision for subsequent tasks.
				go tm.callback(task.NodeID)
				continue
			} else {
				// Calculate remaining duration for the future task.
				delay = nextTask.FireTime.Sub(now)
				hasTask = true
			}
		}
		tm.mu.Unlock()

		if hasTask {
			timer.Reset(delay)
			select {
			case <-timer.C:
				// Window reached; loop to process ready tasks.
			case <-tm.wakeup:
				// Interrupted by a new schedule; recalibrate wait time.
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			case <-tm.ctx.Done():
				timer.Stop()
				return
			}
		} else {
			// Idle state: wait for a new task or shutdown signal.
			select {
			case <-tm.wakeup:
			case <-tm.ctx.Done():
				return
			}
		}
	}
}

// Stop terminates the manager and blocks until the background loop completes its exit.
func (tm *TimerManager) Stop() {
	tm.cancel()
	tm.wg.Wait()
}
