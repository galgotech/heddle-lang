package state

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimerManager_ScheduleAndFire(t *testing.T) {
	var mu sync.Mutex
	fired := make(map[string]bool)

	callback := func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		fired[nodeID] = true
	}

	tm := NewTimerManager(callback)
	defer tm.Stop()

	// Schedule a timer for 50ms
	tm.Schedule("node-1", 50*time.Millisecond)

	// Should not have fired immediately
	mu.Lock()
	assert.False(t, fired["node-1"])
	mu.Unlock()

	// Wait for 100ms
	time.Sleep(100 * time.Millisecond)

	// Should have fired
	mu.Lock()
	assert.True(t, fired["node-1"])
	mu.Unlock()
}

func TestTimerManager_ConcurrentSchedules(t *testing.T) {
	var mu sync.Mutex
	firedCount := 0

	callback := func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		firedCount++
	}

	tm := NewTimerManager(callback)
	defer tm.Stop()

	// Schedule 100 timers concurrently
	numTimers := 100
	var wg sync.WaitGroup
	wg.Add(numTimers)

	for i := 0; i < numTimers; i++ {
		go func(id int) {
			defer wg.Done()
			// Random delay between 10ms and 50ms
			delay := time.Duration(10+(id%40)) * time.Millisecond
			tm.Schedule("node-many", delay)
		}(i)
	}

	wg.Wait()

	// Wait enough time for all to fire
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, numTimers, firedCount)
	mu.Unlock()
}

func TestTimerManager_Stop(t *testing.T) {
	var mu sync.Mutex
	fired := false

	callback := func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		fired = true
	}

	tm := NewTimerManager(callback)

	// Schedule far in the future
	tm.Schedule("node-future", 5*time.Second)

	// Stop immediately
	tm.Stop()

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	assert.False(t, fired, "Timer should not fire after manager is stopped")
	mu.Unlock()
}

func TestTimerManager_WakeupUpdatesTimer(t *testing.T) {
	var mu sync.Mutex
	fired := make(map[string]bool)
	fireTime := make(map[string]time.Time)

	callback := func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		fired[nodeID] = true
		fireTime[nodeID] = time.Now()
	}

	tm := NewTimerManager(callback)
	defer tm.Stop()

	start := time.Now()

	// Schedule one for 200ms
	tm.Schedule("long", 200*time.Millisecond)

	time.Sleep(10 * time.Millisecond)

	// Schedule one for 50ms (should update the internal timer to fire earlier)
	tm.Schedule("short", 50*time.Millisecond)

	// Wait 100ms
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.True(t, fired["short"], "Short timer should have fired")
	assert.False(t, fired["long"], "Long timer should not have fired yet")

	// Check the accuracy
	diff := fireTime["short"].Sub(start)
	assert.True(t, diff >= 50*time.Millisecond, "Short timer should take at least 50ms")
	assert.True(t, diff < 150*time.Millisecond, "Short timer should not take too long")
	mu.Unlock()
}
