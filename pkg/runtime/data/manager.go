package data

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/ipc"
)

// DataManager handles memory allocation, sharing, and spilling of HeddleFrames.
type DataManager struct {
	basePath    string
	memoryLimit int64
	activeBytes int64
	registry    *FrameRegistry
	mu          sync.RWMutex
}

// NewDataManager creates a new DataManager with a memory limit and spill path.
func NewDataManager(basePath string, memoryLimit int64) *DataManager {
	_ = os.MkdirAll(basePath, 0777)
	return &DataManager{
		basePath:    basePath,
		memoryLimit: memoryLimit,
		registry:    NewFrameRegistry(),
	}
}

// Put writes an Arrow Record to the managed storage.
// It decides whether to use RAM (memfd), SHM, or Disk based on memory limits.
func (m *DataManager) Put(id string, record arrow.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Create memfd or file
	// Note: In a full implementation, we might estimate size or use a temp buffer
	// but for Phase 2, we prioritize direct FD writing if possible.
	// We'll use a conservative estimate or a growing file.

	// For now, we still need to know the size for memfd_create if we want it to be contiguous
	// but memfd can be truncated later.
	// Let's use a two-pass approach only if necessary, or just write to FD.

	var f *os.File
	var err error

	// Create with an initial size or grow as needed
	if supportsMemfd() {
		f, err = createMemfd(id, 0)
	} else {
		path := filepath.Join(m.basePath, id)
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	}

	if err != nil {
		return fmt.Errorf("failed to allocate storage: %w", err)
	}

	// 2. Write Arrow Record directly to FD
	writer := ipc.NewWriter(f, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		f.Close()
		return fmt.Errorf("failed to write record: %w", err)
	}
	if err := writer.Close(); err != nil {
		f.Close()
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Get final size
	fi, _ := f.Stat()
	size := fi.Size()

	var loc StorageLocation = LocationShared
	if m.memoryLimit > 0 && m.activeBytes+size > m.memoryLimit {
		loc = LocationDisk
	}

	// Create frame
	var frame *ArrowFrame
	if loc == LocationDisk {
		frame = &ArrowFrame{
			record:   record,
			location: LocationDisk,
			handle:   f.Name(),
			metadata: make(map[string]string),
		}
	} else {
		frame = NewSharedFrame(record, f.Name())
		m.activeBytes += size
	}

	m.registry.Register(id, frame, f)
	return nil
}

// Get retrieves an Arrow Record from storage.
func (m *DataManager) Get(id string) (arrow.Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	frame := m.registry.Get(id)
	if frame == nil {
		return nil, fmt.Errorf("frame not found: %s", id)
	}

	var f *os.File
	var err error

	// Try to get open file from registry first
	f = m.registry.GetFile(id)
	if f == nil {
		// Fallback to opening by handle (for non-memfd or cross-process)
		f, err = os.Open(frame.Handle())
		if err != nil {
			return nil, fmt.Errorf("failed to open frame storage: %w", err)
		}
		defer f.Close()
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat frame storage: %w", err)
	}

	size := fi.Size()
	mmap, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap: %w", err)
	}
	defer syscall.Munmap(mmap)

	reader, err := ipc.NewReader(bytes.NewReader(mmap))
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		return nil, fmt.Errorf("no record in storage")
	}

	rec := reader.Record()
	rec.Retain()
	return rec, nil
}

// GetRegistry returns the frame registry.
func (m *DataManager) GetRegistry() *FrameRegistry {
	return m.registry
}

// Cleanup removes all resources.
func (m *DataManager) Cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Release all registered frames
	for id := range m.registry.frames {
		m.registry.Release(id)
	}

	_ = os.RemoveAll(m.basePath)
	m.activeBytes = 0
}
