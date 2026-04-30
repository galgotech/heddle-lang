package core

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/ipc"
)

// Table is a high-performance data wrapper.
// It is designed to encapsulate apache/arrow/go memory buffers (zero-copy)
// to prevent massive JSON serialization payloads over gRPC.
type Table struct {
	Record arrow.Record
}

// NewTableFromRecord creates a new Table wrapping an Arrow Record.
func NewTableFromRecord(record arrow.Record) *Table {
	if record != nil {
		record.Retain()
	}
	return &Table{
		Record: record,
	}
}

// Release releases the underlying Arrow Record.
func (t *Table) Release() {
	if t.Record != nil {
		t.Record.Release()
		t.Record = nil
	}
}

// NewTableFromBytes creates a new Table from a byte slice containing Arrow IPC data.
func NewTableFromBytes(data []byte) (*Table, error) {
	if len(data) == 0 {
		return &Table{}, nil
	}

	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		return &Table{}, nil
	}

	rec := reader.Record()
	rec.Retain()
	return NewTableFromRecord(rec), nil
}

// ToBytes returns the Arrow IPC representation of the table.
func (t *Table) ToBytes() ([]byte, error) {
	if t.Record == nil {
		return []byte{}, nil
	}

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(t.Record.Schema()))
	if err := writer.Write(t.Record); err != nil {
		return nil, fmt.Errorf("failed to write record: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	return buf.Bytes(), nil
}

// WriteToHandle writes the table to a file handle using Arrow IPC.
func (t *Table) WriteToHandle(handle string) error {
	return WriteTableToHandle(handle, t)
}
