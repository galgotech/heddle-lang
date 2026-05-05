package data

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
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
	allocator       MemoryAllocator
	memoryLimit     int64
	activeBytes     int64
	registry        *TableRegistry
	attachedRegions map[string]MemoryRegion
	mu              sync.RWMutex
}

// NewLocalMmapManager initializes a LocalMmapManager with a specific allocator.
func NewLocalMmapManager(allocator MemoryAllocator, memoryLimit int64) *LocalMmapManager {
	return &LocalMmapManager{
		allocator:       allocator,
		memoryLimit:     memoryLimit,
		registry:        NewTableRegistry(),
		attachedRegions: make(map[string]MemoryRegion),
	}
}

// Get retrieves an Arrow Record from the managed memory regions.
func (m *LocalMmapManager) Get(id string) (arrow.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	table, ok := m.registry.Get(id).(*ArrowTable)
	if !ok || table == nil {
		return nil, fmt.Errorf("table not found: %s", id)
	}

	region, ok := m.attachedRegions[id]
	if !ok {
		var err error
		region, err = m.allocator.Attach(id, table.Handle())
		if err != nil {
			return nil, fmt.Errorf("failed to attach memory region: %w", err)
		}
		m.attachedRegions[id] = region
	}

	mmapData := region.Data()
	origRec := table.Record()
	if origRec == nil {
		return nil, fmt.Errorf("table has no record")
	}

	// If the record is already backed by the correct mmap region (e.g. via Import with IPC),
	// or if we want to bypass reconstruction for legacy reasons.
	if table.Metadata()["format"] == "ipc" {
		origRec.Retain()
		return origRec, nil
	}

	cols := make([]arrow.Array, origRec.NumCols())
	var offset int
	visited := make(map[*array.Data]arrow.ArrayData)

	for i := 0; i < int(origRec.NumCols()); i++ {
		origCol := origRec.Column(i)
		newData := cloneDataWithMmap(origCol.Data(), mmapData, &offset, visited, 0)
		cols[i] = array.MakeFromData(newData)
		newData.Release()
	}

	rec := array.NewRecord(origRec.Schema(), cols, origRec.NumRows())
	return rec, nil
}

// Put persists an Arrow Record to shared memory using optimized mmap writes.
func (m *LocalMmapManager) Put(id string, record arrow.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	size := computeRecordSize(record)
	if size == 0 {
		return fmt.Errorf("cannot store empty record")
	}
	region, err := m.allocator.Allocate(id, size)
	if err != nil {
		return err
	}

	mmapData := region.Data()
	if int64(len(mmapData)) < size {
		return fmt.Errorf("allocated region too small: %d < %d", len(mmapData), size)
	}
	var offset int
	writeRecordBuffers(record, mmapData, &offset)

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
	table.Metadata()["format"] = "ipc"
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
	for id, region := range m.attachedRegions {
		_ = region.Unmap()
		delete(m.attachedRegions, id)
	}
	m.activeBytes = 0
}

func computeRecordSize(record arrow.Record) int64 {
	var size int64
	visited := make(map[*array.Data]bool)
	for i := 0; i < int(record.NumCols()); i++ {
		size += computeDataSize(record.Column(i).Data(), visited, 0)
	}
	return size
}

func computeDataSize(data arrow.ArrayData, visited map[*array.Data]bool, depth int) int64 {
	if data == nil {
		return 0
	}
	d, ok := data.(*array.Data)
	if !ok {
		return 0
	}
	if visited[d] {
		return 0
	}
	visited[d] = true
	var size int64
	for _, buf := range d.Buffers() {
		if buf != nil {
			length := buf.Len()
			size += int64(length)
			// Align to 64 bytes
			if padding := length % 64; padding != 0 {
				size += int64(64 - padding)
			}
		}
	}
	if d.DataType().ID() == arrow.DICTIONARY {
		if dict := d.Dictionary(); dict != nil {
			size += computeDataSize(dict, visited, depth+1)
		}
	} else {
		for _, child := range d.Children() {
			size += computeDataSize(child, visited, depth+1)
		}
	}
	return size
}

func writeRecordBuffers(record arrow.Record, mmapData []byte, offset *int) {
	visited := make(map[*array.Data]bool)
	for i := 0; i < int(record.NumCols()); i++ {
		writeBuffers(record.Column(i).Data(), mmapData, offset, visited, 0)
	}
}

func writeBuffers(data arrow.ArrayData, mmapData []byte, offset *int, visited map[*array.Data]bool, depth int) {
	if data == nil {
		return
	}
	d, ok := data.(*array.Data)
	if !ok {
		return
	}
	if visited[d] {
		return
	}
	visited[d] = true
	for _, buf := range d.Buffers() {
		if buf != nil {
			length := buf.Len()
			copy(mmapData[*offset:], buf.Bytes())
			*offset += length
			// Align to 64 bytes
			if padding := length % 64; padding != 0 {
				*offset += (64 - padding)
			}
		}
	}
	if d.DataType().ID() == arrow.DICTIONARY {
		if dict := d.Dictionary(); dict != nil {
			writeBuffers(dict, mmapData, offset, visited, depth+1)
		}
	} else {
		for _, child := range d.Children() {
			writeBuffers(child, mmapData, offset, visited, depth+1)
		}
	}
}

func cloneDataWithMmap(orig arrow.ArrayData, mmapData []byte, offset *int, visited map[*array.Data]arrow.ArrayData, depth int) arrow.ArrayData {
	if orig == nil {
		return nil
	}
	d, ok := orig.(*array.Data)
	if !ok {
		return nil
	}
	if res, ok := visited[d]; ok {
		return res
	}

	origBufs := d.Buffers()
	newBufs := make([]*memory.Buffer, len(origBufs))
	for i, buf := range origBufs {
		if buf != nil {
			length := buf.Len()
			newBufs[i] = memory.NewBufferBytes(mmapData[*offset : *offset+length])
			*offset += length
			// Align to 64 bytes
			if padding := length % 64; padding != 0 {
				*offset += (64 - padding)
			}
		}
	}

	var res arrow.ArrayData
	if d.DataType().ID() == arrow.DICTIONARY {
		dict := d.Dictionary()
		clonedDict := cloneDataWithMmap(dict, mmapData, offset, visited, depth+1)
		res = array.NewDataWithDictionary(
			d.DataType(), d.Len(), newBufs, d.NullN(), d.Offset(), clonedDict.(*array.Data))
	} else {
		origChildren := d.Children()
		newChildren := make([]arrow.ArrayData, len(origChildren))
		for i, child := range origChildren {
			newChildren[i] = cloneDataWithMmap(child, mmapData, offset, visited, depth+1)
		}
		res = array.NewData(
			d.DataType(), d.Len(), newBufs, newChildren, d.NullN(), d.Offset())
	}
	visited[d] = res
	return res
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
