package core

// Table is a high-performance data wrapper.
// It is designed to encapsulate apache/arrow/go memory buffers (zero-copy)
// to prevent massive JSON serialization payloads over gRPC.
//
// For now, it provides a placeholder implementation to satisfy the Data Bridge requirement.
type Table struct {
	// Future: Encapsulate apache/arrow/go/arrow.Record here
	arrowBuffer []byte
}

// NewTable creates a new, empty Table.
func NewTable() *Table {
	return &Table{
		arrowBuffer: []byte{},
	}
}

// NewTableFromBytes creates a new Table initialized with a byte slice.
func NewTableFromBytes(data []byte) *Table {
	return &Table{
		arrowBuffer: data,
	}
}

// ToBytes returns the underlying byte slice.
func (t *Table) ToBytes() []byte {
	return t.arrowBuffer
}
