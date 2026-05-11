package locality

import (
	"sync"

	"github.com/apache/arrow/go/v18/arrow"
)

type IODirection int

const (
	Input IODirection = iota
	Output
)

type Metadata struct {
	IODirection IODirection
}

// DataLocalityRegistry manages the mapping of data identifiers to their physical locations.
// It acts as an optimized memory-mapping subsystem for zero-copy data flow.
type DataLocalityRegistry struct {
	// storage maps data identifiers (e.g. task output names) to their memory handles or physical locations.
	table    sync.Map
	metadata sync.Map
}

// Put registers a data identifier with its corresponding location/handle.
func (r *DataLocalityRegistry) Put(id string, metadata Metadata, data arrow.Table) {
	r.table.Store(id, data)
	r.metadata.Store(id, metadata)
}

// GetData retrieves the metadata associated with a data identifier.
func (r *DataLocalityRegistry) GetData(id string) (arrow.Table, bool) {
	data, ok := r.table.Load(id)
	if !ok {
		return nil, false
	}

	table, ok := data.(arrow.Table)
	if !ok {
		return nil, false
	}

	return table, true
}

// GetMetadata retrieves the metadata associated with a data identifier.
func (r *DataLocalityRegistry) GetMetadata(id string) (Metadata, bool) {
	metadata, ok := r.metadata.Load(id)
	if !ok {
		return Metadata{}, false
	}

	return metadata.(Metadata), true
}

// Delete removes a data identifier from the registry.
func (r *DataLocalityRegistry) Delete(id string) {
	r.table.Delete(id)
	r.metadata.Delete(id)
}

// NewDataLocalityRegistry initializes a new registry.
func NewDataLocalityRegistry() *DataLocalityRegistry {
	return &DataLocalityRegistry{}
}
