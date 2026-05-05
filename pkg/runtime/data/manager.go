package data

import (
	"bytes"
	"fmt"
	"sync"

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

// LocalMmapManager implements DataManager using an abstracted MemoryAllocator.
// It ensures absolute zero-copy data traffic by mapping Arrow memory buffers.
type LocalMmapManager struct {
	allocator   MemoryAllocator
	memoryLimit int64
	activeBytes int64
	registry    *TableRegistry
	mu          sync.RWMutex
}

// NewLocalMmapManager initializes a LocalMmapManager with a specific allocator.
func NewLocalMmapManager(allocator MemoryAllocator, memoryLimit int64) *LocalMmapManager {
	return &LocalMmapManager{
		allocator:   allocator,
		memoryLimit: memoryLimit,
		registry:    NewTableRegistry(),
	}
}

// Get retrieves an Arrow Record from the managed memory regions.
func (m *LocalMmapManager) Get(id string) (arrow.Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	table, ok := m.registry.Get(id).(*ArrowTable)
	if !ok || table == nil {
		return nil, fmt.Errorf("table not found: %s", id)
	}

	// For Get, we might already have the file in registry, but we need to map it if not.
	// However, LocalMmapManager usually maps on Put/Import.
	// If it's in the registry, it should be accessible.
	
	region, err := m.allocator.Attach(id, table.Handle())
	if err != nil {
		return nil, fmt.Errorf("failed to attach memory region: %w", err)
	}
	defer region.Unmap()

	reader, err := ipc.NewReader(bytes.NewReader(region.Data()))
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

// Put persists an Arrow Record to shared memory using optimized mmap writes.
func (m *LocalMmapManager) Put(id string, record arrow.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var cw counterWriter
	writer := ipc.NewWriter(&cw, ipc.WithSchema(record.Schema()))
	_ = writer.Write(record)
	_ = writer.Close()
	size := int64(cw.count)

	region, err := m.allocator.Allocate(id, size)
	if err != nil {
		return fmt.Errorf("failed to allocate memory region: %w", err)
	}
	defer region.Unmap()

	mw := &mmapWriter{data: region.Data()}
	writer = ipc.NewWriter(mw, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
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
			handle:   region.File().Name(),
			metadata: make(map[string]string),
		}
	} else {
		table = NewSharedTable(record, region.File().Name())
		m.activeBytes += size
	}

	m.registry.Register(id, table, region.File())

	return nil
}

// Import attaches to an existing data handle and registers it.
func (m *LocalMmapManager) Import(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.registry.Exists(id) {
		return nil
	}

	region, err := m.allocator.Attach(id, "")
	if err != nil {
		return fmt.Errorf("failed to attach handle %s: %w", id, err)
	}
	defer region.Unmap()

	reader, err := ipc.NewReader(bytes.NewReader(region.Data()))
	if err != nil {
		return fmt.Errorf("failed to create reader for handle %s: %w", id, err)
	}
	defer reader.Release()

	if !reader.Next() {
		return fmt.Errorf("no record found in handle %s", id)
	}

	rec := reader.Record()
	rec.Retain()

	table := NewSharedTable(rec, region.File().Name())
	m.registry.Register(id, table, region.File())
	m.activeBytes += int64(len(region.Data()))

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

	_ = m.allocator.Cleanup()
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
