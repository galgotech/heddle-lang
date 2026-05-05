package core

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/ipc"
)

// arrowTable is the concrete implementation of the Table interface.
type arrowTable struct {
	record arrow.Record
}

func (t *arrowTable) Native() arrow.Record {
	return t.record
}

func (t *arrowTable) Release() {
	if t.record != nil {
		t.record.Release()
		t.record = nil
	}
}

func (t *arrowTable) ToBytes() ([]byte, error) {
	if t.record == nil {
		return []byte{}, nil
	}

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(t.record.Schema()))
	if err := writer.Write(t.record); err != nil {
		return nil, fmt.Errorf("failed to write record: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	return buf.Bytes(), nil
}

func (t *arrowTable) WriteToHandle(handle string) error {
	return WriteTableToHandle(handle, t)
}

// NewTableFromRecord creates a new Table wrapping an Arrow Record.
func NewTableFromRecord(record arrow.Record) Table {
	if record != nil {
		record.Retain()
	}
	return &arrowTable{
		record: record,
	}
}

// NewTableFromBytes creates a new Table from a byte slice containing Arrow IPC data.
func NewTableFromBytes(data []byte) (Table, error) {
	if len(data) == 0 {
		return &arrowTable{}, nil
	}

	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		return &arrowTable{}, nil
	}

	rec := reader.Record()
	rec.Retain()
	return NewTableFromRecord(rec), nil
}
