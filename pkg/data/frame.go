package data

import (
	"github.com/apache/arrow/go/v18/arrow"
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
}

// ArrowFrame is the default implementation of HeddleFrame using Apache Arrow.
type ArrowFrame struct {
	record arrow.Record
	handle string
}

// NewArrowFrame creates a new HeddleFrame from an Arrow Record.
func NewArrowFrame(record arrow.Record) *ArrowFrame {
	return &ArrowFrame{record: record}
}

// NewSharedFrame creates a new HeddleFrame that resides in shared memory.
func NewSharedFrame(record arrow.Record, handle string) *ArrowFrame {
	return &ArrowFrame{record: record, handle: handle}
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
