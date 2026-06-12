package registry

import (
	"maps"
	"sync"
	"time"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

type WorkerRegistry struct {
	workersMu    sync.RWMutex
	workers      map[string]*WorkerStream
	capabilities map[string]map[string]*WorkerStream // capability -> workerID -> WorkerStream

	clientsMu sync.RWMutex
	clients   map[string]*ClientStream

	resultsMu sync.RWMutex
	results   map[string]chan models.TaskResult

	workflowsMu sync.RWMutex
	workflows   map[string]string // workflowID -> clientID
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		workers:      make(map[string]*WorkerStream),
		capabilities: make(map[string]map[string]*WorkerStream),
		clients:      make(map[string]*ClientStream),
		results:      make(map[string]chan models.TaskResult),
		workflows:    make(map[string]string),
	}
}

func (r *WorkerRegistry) Register(id string, reg models.WorkerRegistration) {
	r.workersMu.Lock()
	defer r.workersMu.Unlock()

	if oldWorker, ok := r.workers[id]; ok {
		oldWorker.workerInfo.mu.RLock()
		oldCaps := oldWorker.workerInfo.Capabilities
		oldWorker.workerInfo.mu.RUnlock()
		for _, c := range oldCaps {
			if r.capabilities[c] != nil {
				delete(r.capabilities[c], id)
				if len(r.capabilities[c]) == 0 {
					delete(r.capabilities, c)
				}
			}
		}
	}

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

func (r *WorkerRegistry) GetActiveWorkerStream(id string) (transport.ExchangeStream, bool) {
	r.workersMu.RLock()
	defer r.workersMu.RUnlock()

	stream, ok := r.workers[id]
	if !ok {
		return nil, false
	}
	s := stream.GetStream()
	if s == nil {
		return nil, false
	}
	return s, true
}

func (r *WorkerRegistry) GetActiveClientStream(id string) (transport.ExchangeStream, bool) {
	r.clientsMu.RLock()
	defer r.clientsMu.RUnlock()

	stream, ok := r.clients[id]
	if !ok || stream.stream == nil {
		return nil, false
	}
	return stream.stream, true
}

func (r *WorkerRegistry) ProcessStream(id string, stream transport.ExchangeStream) bool {
	r.workersMu.RLock()
	defer r.workersMu.RUnlock()

	val, ok := r.workers[id]
	if !ok {
		return false
	}
	val.ProcessStream(stream)

	return true
}

func (r *WorkerRegistry) StopStream(id string) bool {
	r.workersMu.Lock()
	defer r.workersMu.Unlock()

	val, ok := r.workers[id]
	if !ok {
		return false
	}

	val.StopStream()
	delete(r.workers, id)

	val.workerInfo.mu.RLock()
	caps := val.workerInfo.Capabilities
	val.workerInfo.mu.RUnlock()
	for _, c := range caps {
		if r.capabilities[c] != nil {
			delete(r.capabilities[c], id)
			if len(r.capabilities[c]) == 0 {
				delete(r.capabilities, c)
			}
		}
	}

	return true
}

func (r *WorkerRegistry) UpdateCapabilities(id string, update models.WorkerCapabilitiesUpdate) bool {
	r.workersMu.Lock()
	defer r.workersMu.Unlock()
	val, ok := r.workers[id]
	if !ok {
		return false
	}

	val.workerInfo.mu.RLock()
	oldCaps := make(map[string]bool)
	for _, c := range val.workerInfo.Capabilities {
		oldCaps[c] = true
	}
	val.workerInfo.mu.RUnlock()

	val.UpdateCapabilities(update)
	val.LastSeen()

	for _, c := range update.Capabilities {
		if !oldCaps[c] {
			if r.capabilities[c] == nil {
				r.capabilities[c] = make(map[string]*WorkerStream)
			}
			r.capabilities[c][id] = val
		}
	}

	return true
}

func (r *WorkerRegistry) Heartbeat(id string, load int) bool {
	r.workersMu.RLock()
	val, ok := r.workers[id]
	r.workersMu.RUnlock()

	if !ok {
		return false
	}
	val.UpdateHeartbeat(load, time.Now())
	return true
}

func (r *WorkerRegistry) FindWorkerByCapability(capability string) *WorkerStream {
	r.workersMu.RLock()
	defer r.workersMu.RUnlock()

	workers, ok := r.capabilities[capability]
	if !ok || len(workers) == 0 {
		return nil
	}

	threshold := time.Now().Add(-15 * time.Second)
	for _, w := range workers {
		if w.GetLastSeen().After(threshold) {
			return w
		}
	}

	return nil
}

func (r *WorkerRegistry) GetRegistryInfo() models.RegistryInfo {
	r.workersMu.RLock()
	defer r.workersMu.RUnlock()

	info := models.RegistryInfo{
		Steps: make(map[string]schema.StepSchemas),
	}

	threshold := time.Now().Add(-30 * time.Second)

	for _, w := range r.workers {
		if w.GetLastSeen().After(threshold) {
			w.workerInfo.mu.RLock()
			maps.Copy(info.Steps, w.workerInfo.Schemas)
			w.workerInfo.mu.RUnlock()
		}
	}

	return info
}

func (r *WorkerRegistry) ProcessClientStream(id string, stream transport.ExchangeStream) bool {
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
	r.results[taskID] = ch
}

func (r *WorkerRegistry) DeregisterResultChan(taskID string) {
	r.resultsMu.Lock()
	defer r.resultsMu.Unlock()
	delete(r.results, taskID)
}

func (r *WorkerRegistry) RouteResult(result models.TaskResult) bool {
	r.resultsMu.RLock()
	ch, ok := r.results[result.TaskID]
	r.resultsMu.RUnlock()
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

func (r *WorkerRegistry) RegisterWorkflowClient(workflowID, clientID string) {
	r.workflowsMu.Lock()
	defer r.workflowsMu.Unlock()
	r.workflows[workflowID] = clientID
}

func (r *WorkerRegistry) DeregisterWorkflowClient(workflowID string) {
	r.workflowsMu.Lock()
	defer r.workflowsMu.Unlock()
	delete(r.workflows, workflowID)
}

func (r *WorkerRegistry) GetClientIDForWorkflow(workflowID string) (string, bool) {
	r.workflowsMu.RLock()
	defer r.workflowsMu.RUnlock()
	clientID, ok := r.workflows[workflowID]
	return clientID, ok
}
