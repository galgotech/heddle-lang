package data

import (
	"github.com/apache/arrow/go/v18/arrow"
)

// StorageLocation defines where the HeddleFrame data is physically stored.
type StorageLocation int

const (
	LocationMemory StorageLocation = iota
	LocationShared
	LocationDisk
)

// HeddleFrame is the core data structure for Heddle pipelines.
// It wraps an Apache Arrow Record to provide a high-level API
// and ensure zero-copy data passing between steps.
type HeddleFrame interface {
	// Record returns the underlying Apache Arrow Record.
	Record() arrow.Record

	// Schema returns the Arrow schema of the frame.
	Schema() *arrow.Schema

	// NumRows returns the number of rows in the frame.
	NumRows() int64

	// NumCols returns the number of columns in the frame.
	NumCols() int

	// Release releases the underlying memory (important for Arrow).
	Release()

	// Handle returns the shared memory identifier if the frame is stored in SHM.
	Handle() string

	// IsShared returns true if the frame is stored in shared memory.
	IsShared() bool

	// Location returns the current storage location of the frame.
	Location() StorageLocation

	// Metadata returns the metadata associated with the frame.
	Metadata() map[string]string
}

// ArrowFrame is the default implementation of HeddleFrame using Apache Arrow.
type ArrowFrame struct {
	record   arrow.Record
	handle   string
	location StorageLocation
	metadata map[string]string
}

// NewArrowFrame creates a new HeddleFrame from an Arrow Record.
func NewArrowFrame(record arrow.Record) *ArrowFrame {
	return &ArrowFrame{
		record:   record,
		location: LocationMemory,
		metadata: make(map[string]string),
	}
}

// NewSharedFrame creates a new HeddleFrame that resides in shared memory.
func NewSharedFrame(record arrow.Record, handle string) *ArrowFrame {
	return &ArrowFrame{
		record:   record,
		handle:   handle,
		location: LocationShared,
		metadata: make(map[string]string),
	}
}

func (f *ArrowFrame) Record() arrow.Record {
	return f.record
}

func (f *ArrowFrame) Schema() *arrow.Schema {
	return f.record.Schema()
}

func (f *ArrowFrame) NumRows() int64 {
	return f.record.NumRows()
}

func (f *ArrowFrame) NumCols() int {
	return int(f.record.NumCols())
}

func (f *ArrowFrame) Release() {
	if f.record != nil {
		f.record.Release()
	}
}

func (f *ArrowFrame) Handle() string {
	return f.handle
}

func (f *ArrowFrame) IsShared() bool {
	return f.handle != ""
}

func (f *ArrowFrame) Location() StorageLocation {
	return f.location
}

func (f *ArrowFrame) Metadata() map[string]string {
	return f.metadata
}
