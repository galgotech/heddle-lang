package manager

import (
	"sync"
)

// DataLocalityRegistry tracks the physical location of generated outputs across the cluster.
// It allows the Control Plane to optimize for zero-copy memory access via UDS where possible.
type DataLocalityRegistry interface {
	// RegisterOutput records that a specific worker produced a resource.
	RegisterOutput(resourceKey string, workerID string)
	// GetProducer returns the ID of the worker that produced the given resource.
	GetProducer(resourceKey string) (string, bool)
	// Invalidate removes a resource key from the registry (e.g., when a worker fails or data is evicted).
	Invalidate(resourceKey string)
}

// MemoryDataLocalityRegistry is an in-memory implementation of DataLocalityRegistry.
type MemoryDataLocalityRegistry struct {
	mu sync.RWMutex
	// resourceToWorker maps ResourceKey (OutputHandle) to the Worker ID that produced it.
	resourceToWorker map[string]string
}

// NewDataLocalityRegistry creates a new instance of MemoryDataLocalityRegistry as the default implementation.
func NewDataLocalityRegistry() DataLocalityRegistry {
	return &MemoryDataLocalityRegistry{
		resourceToWorker: make(map[string]string),
	}
}

// RegisterOutput records that a specific worker produced a resource.
func (r *MemoryDataLocalityRegistry) RegisterOutput(resourceKey string, workerID string) {
	if resourceKey == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resourceToWorker[resourceKey] = workerID
}

// GetProducer returns the ID of the worker that produced the given resource.
func (r *MemoryDataLocalityRegistry) GetProducer(resourceKey string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	workerID, exists := r.resourceToWorker[resourceKey]
	return workerID, exists
}

// Invalidate removes a resource key from the registry.
func (r *MemoryDataLocalityRegistry) Invalidate(resourceKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.resourceToWorker, resourceKey)
}
