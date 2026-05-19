package registry

import (
	"maps"
	"sync"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

type WorkerRegistry struct {
	wokersMu sync.RWMutex
	workers  map[string]*WorkerStream

	resultsMu sync.RWMutex
	results   map[string]chan models.TaskResult
}

func (r *WorkerRegistry) Register(id string, reg models.WorkerRegistration) {
	r.wokersMu.Lock()
	defer r.wokersMu.Unlock()

	r.workers[id] = &WorkerStream{
		workerInfo: workerInfo{
			ID:           id,
			Registration: reg,
			Schemas:      make(map[string]schema.StepSchemas),
		},
		lastSeen:    time.Now(),
		activeTasks: 0,
	}
}

func (r *WorkerRegistry) ProcessStream(id string, stream flight.FlightService_DoExchangeServer) bool {
	r.wokersMu.RLock()
	defer r.wokersMu.RUnlock()

	val, ok := r.workers[id]
	if !ok {
		return false
	}
	val.ProcessStream(stream)

	return true
}

func (r *WorkerRegistry) GetActiveStream(id string) (flight.FlightService_DoExchangeServer, bool) {
	r.wokersMu.RLock()
	defer r.wokersMu.RUnlock()

	stream, ok := r.workers[id]
	if !ok {
		return nil, false
	}
	return stream.stream, true
}

func (r *WorkerRegistry) StopStream(id string) bool {
	r.wokersMu.Lock()
	defer r.wokersMu.Unlock()

	val, ok := r.workers[id]
	if !ok {
		return false
	}

	val.StopStream()
	return true
}

func (r *WorkerRegistry) UpdateCapabilities(id string, update models.WorkerCapabilitiesUpdate) bool {
	r.wokersMu.RLock()
	defer r.wokersMu.RUnlock()
	val, ok := r.workers[id]
	if !ok {
		return false
	}

	val.UpdateCapabilities(update)
	val.LastSeen()
	return true
}

func (r *WorkerRegistry) Heartbeat(id string, load int) bool {
	r.wokersMu.RLock()
	defer r.wokersMu.RUnlock()

	val, ok := r.workers[id]
	if !ok {
		return false
	}
	val.lastSeen = time.Now()
	val.activeTasks = load
	return true
}

func (r *WorkerRegistry) FindWorkerStreamForStep(capability string) *WorkerStream {
	r.wokersMu.RLock()
	defer r.wokersMu.RUnlock()

	threshold := time.Now().Add(-15 * time.Second)
	for _, w := range r.workers {
		if w.lastSeen.After(threshold) {
			return w
		}
	}

	return nil
}

func (r *WorkerRegistry) GetRegistryInfo() models.RegistryInfo {
	r.wokersMu.RLock()
	defer r.wokersMu.RUnlock()

	info := models.RegistryInfo{
		Steps: make(map[string]schema.StepSchemas),
	}

	threshold := time.Now().Add(-30 * time.Second)

	for _, w := range r.workers {
		w.workerInfo.mu.RLock()
		lastSeen := w.lastSeen
		if lastSeen.After(threshold) {
			maps.Copy(info.Steps, w.workerInfo.Schemas)
		}
		w.workerInfo.mu.RUnlock()
	}

	return info
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		workers: make(map[string]*WorkerStream),
	}
}
