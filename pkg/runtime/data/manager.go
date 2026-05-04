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
// Import registers an existing handle from shared memory.
// It opens the file, reads the Arrow metadata, and creates a HeddleFrame.
func (m *DataManager) Import(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.registry.Exists(id) {
		return nil // Already registered
	}

	path := filepath.Join(m.basePath, id)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open handle %s: %w", id, err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("failed to stat handle %s: %w", id, err)
	}

	size := fi.Size()
	if size == 0 {
		f.Close()
		return fmt.Errorf("handle %s is empty", id)
	}

	// Mmap to read the record without copying
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return fmt.Errorf("failed to mmap handle %s: %w", id, err)
	}
	// We don't munmap here if we want the record to stay valid,
	// but Arrow's ipc.Reader might need the buffer to stay alive.
	// Actually, Arrow Record retains the underlying memory if managed correctly.
	// For simplicity in this implementation, we read the record and keep it in memory.

	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		syscall.Munmap(data)
		f.Close()
		return fmt.Errorf("failed to create reader for handle %s: %w", id, err)
	}
	defer reader.Release()

	if !reader.Next() {
		syscall.Munmap(data)
		f.Close()
		return fmt.Errorf("no record found in handle %s", id)
	}

	rec := reader.Record()
	rec.Retain()

	// Register the frame
	frame := NewSharedFrame(rec, f.Name())
	m.registry.Register(id, frame, f)

	// Note: activeBytes tracking for imported frames
	m.activeBytes += size

	return nil
}

func (m *DataManager) Put(id string, record arrow.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Calculate the required size for the Arrow IPC stream.
	// We use a counter writer to determine the exact size without extra heap allocations.
	var cw counterWriter
	w := ipc.NewWriter(&cw, ipc.WithSchema(record.Schema()))
	if err := w.Write(record); err != nil {
		return fmt.Errorf("failed to calculate record size: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to finalize size calculation: %w", err)
	}
	size := int64(cw.count)

	// 2. Allocate storage (memfd or file)
	var f *os.File
	var err error
	if supportsMemfd() {
		f, err = createMemfd(id, size)
	} else {
		path := filepath.Join(m.basePath, id)
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err == nil {
			err = f.Truncate(size)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to allocate storage: %w", err)
	}

	// 3. Mmap the file for writing.
	// This achieves zero-copy (single copy from heap to mmap) by mapping the file
	// directly into the address space and avoiding write() syscalls.
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return fmt.Errorf("failed to mmap for writing: %w", err)
	}
	defer syscall.Munmap(data)

	// 4. Write Arrow Record directly into the mapped region.
	mw := &mmapWriter{data: data}
	writer := ipc.NewWriter(mw, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		f.Close()
		return fmt.Errorf("failed to write record to mmap: %w", err)
	}
	if err := writer.Close(); err != nil {
		f.Close()
		return fmt.Errorf("failed to close mmap writer: %w", err)
	}

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

// counterWriter counts bytes written without storing them.
type counterWriter struct {
	count int
}

func (c *counterWriter) Write(p []byte) (n int, err error) {
	c.count += len(p)
	return len(p), nil
}

// mmapWriter writes directly into a mapped memory region.
type mmapWriter struct {
	data []byte
	pos  int
}

func (w *mmapWriter) Write(p []byte) (n int, err error) {
	if w.pos+len(p) > len(w.data) {
		return 0, fmt.Errorf("mmapWriter: write out of bounds")
	}
	copy(w.data[w.pos:], p)
	w.pos += len(p)
	return len(p), nil
}
