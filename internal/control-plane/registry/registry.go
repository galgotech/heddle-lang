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

	clientsMu sync.RWMutex
	clients   map[string]*ClientStream

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
		registry:    r,
	}
}

func (r *WorkerRegistry) GetActiveWorkerStream(id string) (flight.FlightService_DoExchangeServer, bool) {
	r.wokersMu.RLock()
	defer r.wokersMu.RUnlock()

	stream, ok := r.workers[id]
	if !ok || stream.stream == nil {
		return nil, false
	}
	return stream.stream, true
}

func (r *WorkerRegistry) GetActiveClientStream(id string) (flight.FlightService_DoExchangeServer, bool) {
	r.clientsMu.RLock()
	defer r.clientsMu.RUnlock()

	stream, ok := r.clients[id]
	if !ok || stream.stream == nil {
		return nil, false
	}
	return stream.stream, true
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

func (r *WorkerRegistry) ProcessClientStream(id string, stream flight.FlightService_DoExchangeServer) bool {
	r.clientsMu.Lock()
	defer r.clientsMu.Unlock()
	r.clients[id] = NewClientStream(stream)
	return true
}

func (r *WorkerRegistry) StopClientStream(id string) bool {
	r.clientsMu.Lock()
	defer r.clientsMu.Unlock()
	delete(r.clients, id)
	return true
}

func (r *WorkerRegistry) RegisterResultChan(taskID string, ch chan models.TaskResult) {
	r.resultsMu.Lock()
	defer r.resultsMu.Unlock()
	if r.results == nil {
		r.results = make(map[string]chan models.TaskResult)
	}
	r.results[taskID] = ch
}

func (r *WorkerRegistry) DeregisterResultChan(taskID string) {
	r.resultsMu.Lock()
	defer r.resultsMu.Unlock()
	delete(r.results, taskID)
}

func (r *WorkerRegistry) RouteResult(result models.TaskResult) bool {
	r.resultsMu.RLock()
	defer r.resultsMu.RUnlock()
	ch, ok := r.results[result.TaskID]
	if !ok {
		return false
	}
	select {
	case ch <- result:
		return true
	default:
		return false
	}
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		workers: make(map[string]*WorkerStream),
		clients: make(map[string]*ClientStream),
		results: make(map[string]chan models.TaskResult),
	}
}
