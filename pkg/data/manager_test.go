package data

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestDataManager_PutGet(t *testing.T) {
	manager := NewDataManager("/dev/shm/heddle-test", 0) // No limit for this test
	defer manager.Cleanup()

	// 1. Create a dummy record
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "val", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20, 30}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	// 2. Put into shared memory
	id := "test-obj-1"
	err := manager.Put(id, rec)
	if err != nil {
		t.Fatalf("failed to put record: %v", err)
	}

	// 3. Get from shared memory
	rec2, err := manager.Get(id)
	if err != nil {
		t.Fatalf("failed to get record: %v", err)
	}
	defer rec2.Release()

	// 4. Verify data
	if rec2.NumRows() != 3 {
		t.Errorf("expected 3 rows, got %d", rec2.NumRows())
	}

	col := rec2.Column(0).(*array.Int64)
	if col.Value(0) != 10 || col.Value(1) != 20 || col.Value(2) != 30 {
		t.Errorf("unexpected values in retrieved record")
	}
}
