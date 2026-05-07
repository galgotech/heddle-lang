package manager

import (
	"sync"
)

// ConcurrencyPool abstracts the execution of parallel tasks within the dispatcher.
// By decoupling from the "go" keyword, we enable deterministic unit testing
// of the orchestration logic without race conditions or environment jitter.
type ConcurrencyPool interface {
	// Go schedules a task for execution.
	Go(task func())
	// Wait blocks until all scheduled tasks have finished.
	Wait()
}

// GoroutinePool implements ConcurrencyPool using native Go routines and WaitGroups.
type GoroutinePool struct {
	wg sync.WaitGroup
}

// NewGoroutinePool initializes a new production concurrency pool.
func NewGoroutinePool() *GoroutinePool {
	return &GoroutinePool{}
}

func (p *GoroutinePool) Go(task func()) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		task()
	}()
}

func (p *GoroutinePool) Wait() {
	p.wg.Wait()
}
