package registry

import (
	"maps"
	"sync"
	"time"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

// WorkerRegistry serves as the central state manager for the control plane.
// It tracks connected worker nodes, external clients, active workflows, and
// maintains a reverse index of capabilities for O(1) scheduling lookups.
type WorkerRegistry struct {
	workersMu    sync.RWMutex
	workers      map[string]*WorkerStream
	// capabilities maps a specific capability name to a map of worker IDs
	// that support it, enabling fast capability-based scheduling.
	capabilities map[string]map[string]*WorkerStream // capability -> workerID -> WorkerStream

	clientsMu sync.RWMutex
	clients   map[string]*ClientStream

	workflowsMu sync.RWMutex
	workflows   map[string]string // workflowID -> clientID
}

// NewWorkerRegistry initializes and returns a new empty WorkerRegistry.
func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		workers:      make(map[string]*WorkerStream),
		capabilities: make(map[string]map[string]*WorkerStream),
		clients:      make(map[string]*ClientStream),
		workflows:    make(map[string]string),
	}
}

// Register registers a new worker or updates an existing one with the given ID
// and registration data. If the worker already exists, its previous capability
// entries are removed from the reverse index before re-registration.
func (r *WorkerRegistry) Register(id string, reg models.WorkerRegistration) {
	r.workersMu.Lock()
	defer r.workersMu.Unlock()

	// Clean up capability indexes for existing workers before overwriting.
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

	// Initialize the new worker stream state.
	r.workers[id] = &WorkerStream{
		workerInfo: workerInfo{
			ID:           id,
			Registration: reg,
			Schemas:      make(map[string]schema.StepSchemas),
		},
		lastSeen:    time.Now(),
		activeTasks: 0,
		registry:    r,
		results:     newShardedResultMap(),
	}
}

// GetActiveClientStream retrieves the transport stream for a given client ID.
// Returns the stream and a boolean indicating if the client is found and active.
func (r *WorkerRegistry) GetActiveClientStream(id string) (transport.ExchangeStream, bool) {
	r.clientsMu.RLock()
	defer r.clientsMu.RUnlock()

	stream, ok := r.clients[id]
	if !ok || stream.stream == nil {
		return nil, false
	}
	return stream.stream, true
}

// ProcessStream attaches a new transport stream to an already registered worker.
// Returns true if the worker exists and the stream was successfully attached.
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

// StopStream stops a worker's stream and entirely removes the worker from the registry,
// including cleaning up its capability entries in the reverse index.
func (r *WorkerRegistry) StopStream(id string) bool {
	r.workersMu.Lock()
	defer r.workersMu.Unlock()

	val, ok := r.workers[id]
	if !ok {
		return false
	}

	val.StopStream()
	delete(r.workers, id)

	// Clean up the worker from the capability reverse index.
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

// UpdateCapabilities updates the active capabilities of a worker and updates the
// reverse index. It only adds new capabilities to the index; it does not remove
// existing ones that are omitted in the update.
func (r *WorkerRegistry) UpdateCapabilities(id string, update models.WorkerCapabilitiesUpdate) bool {
	r.workersMu.Lock()
	defer r.workersMu.Unlock()
	val, ok := r.workers[id]
	if !ok {
		return false
	}

	// Capture existing capabilities to avoid duplicate additions to the index.
	val.workerInfo.mu.RLock()
	oldCaps := make(map[string]bool)
	for _, c := range val.workerInfo.Capabilities {
		oldCaps[c] = true
	}
	val.workerInfo.mu.RUnlock()

	val.UpdateCapabilities(update)
	val.LastSeen()

	// Add any newly reported capabilities to the reverse index.
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

// Heartbeat records a ping from a worker, updating its last seen timestamp
// and current load metrics for health monitoring and scheduling decisions.
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

// FindWorkerByCapability returns an active worker that supports the requested
// capability. It enforces a 15-second freshness threshold on the worker's heartbeat.
// Returns nil if no suitable active worker is found.
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

// GetRegistryInfo aggregates and returns the combined step schemas of all workers
// that have sent a heartbeat within the last 30 seconds.
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

// ProcessClientStream registers a new transport stream for a client, enabling
// bi-directional communication between the control plane and the client.
func (r *WorkerRegistry) ProcessClientStream(id string, stream transport.ExchangeStream) bool {
	r.clientsMu.Lock()
	defer r.clientsMu.Unlock()
	r.clients[id] = NewClientStream(stream)
	return true
}

// StopClientStream disconnects a client and removes it from the registry.
func (r *WorkerRegistry) StopClientStream(id string) bool {
	r.clientsMu.Lock()
	defer r.clientsMu.Unlock()
	delete(r.clients, id)
	return true
}

// RegisterWorkflowClient associates a workflow ID with a client ID. This mapping
// is used to route execution results and debug events back to the correct client.
func (r *WorkerRegistry) RegisterWorkflowClient(workflowID, clientID string) {
	r.workflowsMu.Lock()
	defer r.workflowsMu.Unlock()
	r.workflows[workflowID] = clientID
}

// DeregisterWorkflowClient removes the association between a workflow and a client,
// typically called when the workflow completes or fails.
func (r *WorkerRegistry) DeregisterWorkflowClient(workflowID string) {
	r.workflowsMu.Lock()
	defer r.workflowsMu.Unlock()
	delete(r.workflows, workflowID)
}

// GetClientIDForWorkflow retrieves the client ID associated with a running workflow.
// Returns the client ID and a boolean indicating if the mapping exists.
func (r *WorkerRegistry) GetClientIDForWorkflow(workflowID string) (string, bool) {
	r.workflowsMu.RLock()
	defer r.workflowsMu.RUnlock()
	clientID, ok := r.workflows[workflowID]
	return clientID, ok
}
