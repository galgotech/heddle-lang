package core

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
)

type table struct {
	record arrow.Record
}

func (t *table) Native() arrow.Record {
	return t.record
}

func (t *table) ToBytes() ([]byte, error) {
	return nil, fmt.Errorf("ToBytes not implemented")
}

func (t *table) Release() {
	if t.record != nil {
		t.record.Release()
	}
}

func (t *table) WriteToHandle(handle string) error {
	return fmt.Errorf("WriteToHandle not implemented")
}

func NewTableFromRecord(record arrow.Record) Table {
	return &table{record: record}
}
