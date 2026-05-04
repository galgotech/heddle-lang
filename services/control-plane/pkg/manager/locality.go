package manager

import (
	"sync"
)

// DataLocalityRegistry tracks the physical location of generated outputs.
type DataLocalityRegistry struct {
	mu sync.RWMutex
	// resourceToWorker maps ResourceKey (OutputHandle) to the Worker ID that produced it.
	resourceToWorker map[string]string
}

// NewDataLocalityRegistry creates a new instance of DataLocalityRegistry.
func NewDataLocalityRegistry() *DataLocalityRegistry {
	return &DataLocalityRegistry{
		resourceToWorker: make(map[string]string),
	}
}

// RegisterOutput records that a specific worker produced a resource.
func (r *DataLocalityRegistry) RegisterOutput(resourceKey string, workerID string) {
	if resourceKey == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resourceToWorker[resourceKey] = workerID
}

// GetProducer returns the ID of the worker that produced the given resource.
func (r *DataLocalityRegistry) GetProducer(resourceKey string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	workerID, exists := r.resourceToWorker[resourceKey]
	return workerID, exists
}
