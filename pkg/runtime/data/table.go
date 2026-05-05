package data

import (
	"github.com/apache/arrow/go/v18/arrow"
)

// StorageLocation defines where the Table data is physically stored.
type StorageLocation int

const (
	LocationMemory StorageLocation = iota
	LocationShared
	LocationDisk
)

// Table represents the universal data interface for Heddle.
// It wraps an Apache Arrow Record to provide zero-copy data passing
// and strictly-typed columnar access.
type Table interface {
	// Record returns the underlying Apache Arrow Record.
	Record() arrow.Record

	// Schema returns the columnar schema of the table.
	Schema() *arrow.Schema

	// NumRows returns the number of rows.
	NumRows() int64

	// NumCols returns the number of columns.
	NumCols() int

	// Release releases the underlying memory.
	Release()

	// Handle returns the shared memory identifier if applicable.
	Handle() string

	// IsShared returns true if the data is in shared memory.
	IsShared() bool

	// Location returns the physical storage location.
	Location() StorageLocation

	// Metadata returns associated key-value metadata.
	Metadata() map[string]string
}

// ArrowTable is the concrete implementation of Table using Apache Arrow.
type ArrowTable struct {
	record   arrow.Record
	handle   string
	location StorageLocation
	metadata map[string]string
}

func NewArrowTable(record arrow.Record) *ArrowTable {
	return &ArrowTable{
		record:   record,
		location: LocationMemory,
		metadata: make(map[string]string),
	}
}

func NewSharedTable(record arrow.Record, handle string) *ArrowTable {
	return &ArrowTable{
		record:   record,
		handle:   handle,
		location: LocationShared,
		metadata: make(map[string]string),
	}
}

func (t *ArrowTable) Record() arrow.Record {
	return t.record
}

func (t *ArrowTable) Schema() *arrow.Schema {
	return t.record.Schema()
}

func (t *ArrowTable) NumRows() int64 {
	return t.record.NumRows()
}

func (t *ArrowTable) NumCols() int {
	return int(t.record.NumCols())
}

func (t *ArrowTable) Release() {
	if t.record != nil {
		t.record.Release()
	}
}

func (t *ArrowTable) Handle() string {
	return t.handle
}

func (t *ArrowTable) IsShared() bool {
	return t.handle != ""
}

func (t *ArrowTable) Location() StorageLocation {
	return t.location
}

func (t *ArrowTable) Metadata() map[string]string {
	return t.metadata
}
