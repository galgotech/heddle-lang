package data

import (
	"os"
	"sync"
)

// TableRegistry manages the lifecycle and reference counting of Tables.
type TableRegistry struct {
	mu     sync.RWMutex
	frames map[string]*registryEntry
}

type registryEntry struct {
	frame    Table
	file     *os.File // Optional: kept open for memfd
	refCount int
}

// NewTableRegistry creates a new instance of the TableRegistry.
func NewTableRegistry() *TableRegistry {
	return &TableRegistry{
		frames: make(map[string]*registryEntry),
	}
}

// Register adds a new table to the registry with an initial refCount of 1.
func (r *TableRegistry) Register(id string, table Table, file *os.File) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.frames[id] = &registryEntry{
		frame:    table,
		file:     file,
		refCount: 1,
	}
}

// Exists checks if a table with the given ID is in the registry.
func (r *TableRegistry) Exists(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.frames[id]
	return ok
}

// Retain increments the reference count for a table.
func (r *TableRegistry) Retain(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, ok := r.frames[id]; ok {
		entry.refCount++
	}
}

// Release decrements the reference count for a table.
// If the count reaches 0, the table is released and removed from the registry.
func (r *TableRegistry) Release(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.frames[id]
	if !ok {
		return
	}

	entry.refCount--
	if entry.refCount <= 0 {
		if entry.file != nil {
			_ = entry.file.Close()
		}
		entry.frame.Release()
		delete(r.frames, id)
	}
}

// RefCount returns the current reference count for a table.
func (r *TableRegistry) RefCount(id string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if entry, ok := r.frames[id]; ok {
		return entry.refCount
	}
	return 0
}

// Get returns the table associated with the given ID.
func (r *TableRegistry) Get(id string) Table {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if entry, ok := r.frames[id]; ok {
		return entry.frame
	}
	return nil
}

// GetFile returns the file associated with the given ID.
func (r *TableRegistry) GetFile(id string) *os.File {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if entry, ok := r.frames[id]; ok {
		return entry.file
	}
	return nil
}
