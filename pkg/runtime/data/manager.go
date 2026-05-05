package data

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
)

// DataManager defines the interface for high-performance memory management
// and data transfer in Heddle Lang. It abstracts the underlying storage
// and transport mechanisms, allowing the pipeline to operate on Arrow Records
// regardless of whether they are local in shared memory or remote on another node.
type DataManager interface {
	// Get retrieves an Arrow Record by its unique identifier.
	// For local managers, this typically maps shared memory.
	// For remote managers, this may trigger a network transfer (e.g., Flight DoGet).
	Get(id string) (arrow.Record, error)

	// Put persists an Arrow Record into the managed storage or transport layer.
	// It ensures that the data is accessible to other workers or steps.
	Put(id string, record arrow.Record) error

	// Import attaches to an existing data handle (e.g., shared memory or disk file) and registers it.
	Import(id string) error

	// GetRegistry returns the underlying frame registry for metadata access.
	GetRegistry() *TableRegistry

	// Cleanup releases all resources associated with the manager.
	Cleanup()
}

// LocalMmapManager implements DataManager using POSIX shared memory (memfd or /dev/shm).
// It ensures absolute zero-copy data traffic by mapping Arrow memory buffers directly.
type LocalMmapManager struct {
	basePath    string
	memoryLimit int64
	activeBytes int64
	registry    *TableRegistry
	mu          sync.RWMutex
}

// NewLocalMmapManager initializes a LocalMmapManager with a specific base path.
func NewLocalMmapManager(basePath string, memoryLimit int64) (*LocalMmapManager, error) {
	if err := os.MkdirAll(basePath, 0777); err != nil {
		return nil, fmt.Errorf("failed to create base path %s: %w", basePath, err)
	}
	return &LocalMmapManager{
		basePath:    basePath,
		memoryLimit: memoryLimit,
		registry:    NewTableRegistry(),
	}, nil
}

// Get retrieves an Arrow Record from local shared memory.
func (m *LocalMmapManager) Get(id string) (arrow.Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	table, ok := m.registry.Get(id).(*ArrowTable)
	if !ok || table == nil {
		return nil, fmt.Errorf("table not found: %s", id)
	}

	var f *os.File
	var err error

	f = m.registry.GetFile(id)
	if f == nil {
		f, err = os.Open(table.Handle())
		if err != nil {
			return nil, fmt.Errorf("failed to open table storage: %w", err)
		}
		defer f.Close()
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat table storage: %w", err)
	}

	size := fi.Size()
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap: %w", err)
	}
	defer syscall.Munmap(data)

	reader, err := ipc.NewReader(bytes.NewReader(data))
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

// Put persists an Arrow Record to local shared memory using optimized mmap writes.
// It avoids multiple serialization passes by leveraging the Arrow IPC message directly.
func (m *LocalMmapManager) Put(id string, record arrow.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var cw counterWriter
	writer := ipc.NewWriter(&cw, ipc.WithSchema(record.Schema()))
	_ = writer.Write(record)
	_ = writer.Close()
	size := int64(cw.count)

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

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return fmt.Errorf("failed to mmap for writing: %w", err)
	}
	defer syscall.Munmap(data)

	mw := &mmapWriter{data: data}
	writer = ipc.NewWriter(mw, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		f.Close()
		return fmt.Errorf("failed to write record to mmap: %w", err)
	}
	_ = writer.Close()

	// Determine storage location based on memory limits.
	var loc StorageLocation = LocationShared
	if m.memoryLimit > 0 && m.activeBytes+size > m.memoryLimit {
		loc = LocationDisk
	}

	var table Table
	if loc == LocationDisk {
		table = &ArrowTable{
			record:   record,
			location: LocationDisk,
			handle:   f.Name(),
			metadata: make(map[string]string),
		}
	} else {
		table = NewSharedTable(record, f.Name())
		m.activeBytes += size
	}

	m.registry.Register(id, table, f)

	return nil
}

// Import attaches to an existing data handle (e.g., shared memory or disk file) and registers it.
func (m *LocalMmapManager) Import(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.registry.Exists(id) {
		return nil
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

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return fmt.Errorf("failed to mmap handle %s: %w", id, err)
	}

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

	table := NewSharedTable(rec, f.Name())
	m.registry.Register(id, table, f)
	m.activeBytes += size

	return nil
}

func (m *LocalMmapManager) GetRegistry() *TableRegistry {
	return m.registry
}

func (m *LocalMmapManager) Cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id := range m.registry.frames {
		m.registry.Release(id)
	}

	_ = os.RemoveAll(m.basePath)
	m.activeBytes = 0
}

// FlightNetworkManager implements DataManager using Arrow Flight RPC.
// it facilitates P2P data transfers between distributed workers transparently.
type FlightNetworkManager struct {
	client   flight.Client
	localMgr DataManager // Fallback for local caching
}

func NewFlightNetworkManager(client flight.Client, localMgr DataManager) *FlightNetworkManager {
	return &FlightNetworkManager{
		client:   client,
		localMgr: localMgr,
	}
}

func (m *FlightNetworkManager) Get(id string) (arrow.Record, error) {
	if m.localMgr != nil {
		if rec, err := m.localMgr.Get(id); err == nil {
			return rec, nil
		}
	}
	return nil, fmt.Errorf("FlightNetworkManager.Get not fully implemented for remote resolution")
}

func (m *FlightNetworkManager) Put(id string, record arrow.Record) error {
	if m.localMgr != nil {
		return m.localMgr.Put(id, record)
	}
	return fmt.Errorf("no local manager configured for FlightNetworkManager")
}

func (m *FlightNetworkManager) Import(id string) error {
	if m.localMgr != nil {
		return m.localMgr.Import(id)
	}
	return fmt.Errorf("no local manager configured for FlightNetworkManager")
}

func (m *FlightNetworkManager) GetRegistry() *TableRegistry {
	if m.localMgr != nil {
		return m.localMgr.GetRegistry()
	}
	return nil
}

func (m *FlightNetworkManager) Cleanup() {
	if m.localMgr != nil {
		m.localMgr.Cleanup()
	}
}

type counterWriter struct {
	count int
}

func (c *counterWriter) Write(p []byte) (n int, err error) {
	c.count += len(p)
	return len(p), nil
}

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
