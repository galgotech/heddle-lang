package locality

import (
	"sync"
)

// LocationMetadata strictly contains the physical location information of an Arrow resource.
type LocationMetadata struct {
	// WorkerID is the unique identifier of the worker holding the data.
	WorkerID string
	// HostAddress is the IP/Port of the host (e.g., 10.0.0.1:50051).
	HostAddress string
	// MemoryHandle is the internal handle for shared memory (e.g., unix socket path or shm segment).
	MemoryHandle string
}

// DataLocalityRegistry acts as the "Brain's" memory, mapping DAG outputs to their physical locations.
// It uses sync.Map for high-performance concurrent access without the need for manual Mutex management.
type DataLocalityRegistry struct {
	locations sync.Map // map[string]LocationMetadata
}

// NewDataLocalityRegistry initializes a new concurrent registry.
func NewDataLocalityRegistry() *DataLocalityRegistry {
	return &DataLocalityRegistry{}
}

// Register adds or updates the location of a resource in the registry.
func (r *DataLocalityRegistry) Register(resourceID string, metadata LocationMetadata) {
	r.locations.Store(resourceID, metadata)
}

// Get retrieves the location metadata for a given resource.
func (r *DataLocalityRegistry) Get(resourceID string) (LocationMetadata, bool) {
	val, ok := r.locations.Load(resourceID)
	if !ok {
		return LocationMetadata{}, false
	}
	return val.(LocationMetadata), true
}
