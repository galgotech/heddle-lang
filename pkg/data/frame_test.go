package data

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestArrowFrame(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	frame := NewArrowFrame(rec)

	if frame.NumRows() != 3 {
		t.Errorf("expected 3 rows, got %d", frame.NumRows())
	}

	if frame.NumCols() != 2 {
		t.Errorf("expected 2 columns, got %d", frame.NumCols())
	}

	if frame.Schema().Field(0).Name != "id" {
		t.Errorf("expected first field name 'id', got %s", frame.Schema().Field(0).Name)
	}
}
