package state

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// TimerTask represents a task to be executed after a delay.
// Pointerless struct to avoid GC pressure.
type TimerTask struct {
	NodeID   string
	FireTime time.Time
}

// timerHeap is a min-heap of TimerTasks.
type timerHeap []TimerTask

func (h timerHeap) Len() int           { return len(h) }
func (h timerHeap) Less(i, j int) bool { return h[i].FireTime.Before(h[j].FireTime) }
func (h timerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *timerHeap) Push(x interface{}) {
	*h = append(*h, x.(TimerTask))
}

func (h *timerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *timerHeap) Peek() *TimerTask {
	if len(*h) == 0 {
		return nil
	}
	return &(*h)[0]
}

// TimerManager manages massively concurrent wait-states.
type TimerManager struct {
	mu       sync.Mutex
	tasks    timerHeap
	wakeup   chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	callback func(nodeID string)
	wg       sync.WaitGroup
}

// NewTimerManager creates a new TimerManager.
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

// Schedule adds a new timer task for the given node ID.
func (tm *TimerManager) Schedule(nodeID string, delay time.Duration) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	task := TimerTask{
		NodeID:   nodeID,
		FireTime: time.Now().Add(delay),
	}
	heap.Push(&tm.tasks, task)

	// Wake up the runner loop to recalculate wait time
	select {
	case tm.wakeup <- struct{}{}:
	default:
	}
}

func (tm *TimerManager) run() {
	defer tm.wg.Done()
	timer := time.NewTimer(0)
	<-timer.C // Drain it initially

	for {
		tm.mu.Lock()
		nextTask := tm.tasks.Peek()
		var delay time.Duration
		hasTask := false

		if nextTask != nil {
			now := time.Now()
			if now.After(nextTask.FireTime) || now.Equal(nextTask.FireTime) {
				// Task is ready, pop it and fire
				task := heap.Pop(&tm.tasks).(TimerTask)
				tm.mu.Unlock()

				// Fire callback asynchronously so we don't block the timer loop
				go tm.callback(task.NodeID)
				continue
			} else {
				// Task is in the future
				delay = nextTask.FireTime.Sub(now)
				hasTask = true
			}
		}
		tm.mu.Unlock()

		if hasTask {
			timer.Reset(delay)
			select {
			case <-timer.C:
				// Timer expired, loop around to check tasks
			case <-tm.wakeup:
				// A new task was added, stop timer and loop around to recalculate
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
			select {
			case <-tm.wakeup:
				// A new task was added
			case <-tm.ctx.Done():
				return
			}
		}
	}
}

// Stop shuts down the TimerManager and waits for the runner loop to exit.
func (tm *TimerManager) Stop() {
	tm.cancel()
	tm.wg.Wait()
}
