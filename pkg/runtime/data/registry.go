package data

import (
	"os"
	"sync"
)

// FrameRegistry manages the lifecycle and reference counting of HeddleFrames.
type FrameRegistry struct {
	mu     sync.RWMutex
	frames map[string]*registryEntry
}

type registryEntry struct {
	frame    HeddleFrame
	file     *os.File // Optional: kept open for memfd
	refCount int
}

// NewFrameRegistry creates a new instance of the FrameRegistry.
func NewFrameRegistry() *FrameRegistry {
	return &FrameRegistry{
		frames: make(map[string]*registryEntry),
	}
}

// Register adds a new frame to the registry with an initial refCount of 1.
func (r *FrameRegistry) Register(id string, frame HeddleFrame, file *os.File) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.frames[id] = &registryEntry{
		frame:    frame,
		file:     file,
		refCount: 1,
	}
}

// Exists checks if a frame with the given ID is in the registry.
func (r *FrameRegistry) Exists(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.frames[id]
	return ok
}

// Retain increments the reference count for a frame.
func (r *FrameRegistry) Retain(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, ok := r.frames[id]; ok {
		entry.refCount++
	}
}

// Release decrements the reference count for a frame.
// If the count reaches 0, the frame is released and removed from the registry.
func (r *FrameRegistry) Release(id string) {
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

// RefCount returns the current reference count for a frame.
func (r *FrameRegistry) RefCount(id string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if entry, ok := r.frames[id]; ok {
		return entry.refCount
	}
	return 0
}

// Get returns the frame associated with the given ID.
func (r *FrameRegistry) Get(id string) HeddleFrame {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if entry, ok := r.frames[id]; ok {
		return entry.frame
	}
	return nil
}

// GetFile returns the file associated with the given ID.
func (r *FrameRegistry) GetFile(id string) *os.File {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if entry, ok := r.frames[id]; ok {
		return entry.file
	}
	return nil
}
