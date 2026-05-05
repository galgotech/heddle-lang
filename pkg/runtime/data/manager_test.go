package data

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestDataManager_PutGet(t *testing.T) {
	tmpDir := t.TempDir()
	alloc := NewOSMemoryAllocator(tmpDir)
	manager := NewLocalMmapManager(alloc, 0) // No limit for this test
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

func TestDataManager_ZeroCopy(t *testing.T) {
	tmpDir := t.TempDir()
	alloc := NewOSMemoryAllocator(tmpDir)
	manager := NewLocalMmapManager(alloc, 0)
	defer manager.Cleanup()

	// 1. Create a complex record with different types
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "ints", Type: arrow.PrimitiveTypes.Int64},
			{Name: "strings", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"heddle", "lang", "zero-copy"}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	// 2. Put record
	id := "zero-copy-obj"
	err := manager.Put(id, rec)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 3. Mathematical Size Assertion (Critical Acceptance Criteria)
	expectedSize := computeRecordSize(rec)
	
	path := filepath.Join(tmpDir, id)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	actualSize := info.Size()
	if actualSize != expectedSize {
		t.Errorf("Zero-Copy Failure: Expected file size %d bytes, got %d. IPC overhead detected!", expectedSize, actualSize)
	} else {
		t.Logf("Architecture Validation Passed: File size %d is exactly the sum of RAM buffers", actualSize)
	}

	// 4. Deep Validation
	rec2, err := manager.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rec2.Release()

	if rec2.NumRows() != rec.NumRows() {
		t.Errorf("row count mismatch: expected %d, got %d", rec.NumRows(), rec2.NumRows())
	}

	// Compare values
	ints := rec2.Column(0).(*array.Int64)
	if ints.Value(0) != 1 || ints.Value(2) != 3 {
		t.Errorf("int value mismatch")
	}

	strs := rec2.Column(1).(*array.String)
	if strs.Value(0) != "heddle" || strs.Value(2) != "zero-copy" {
		t.Errorf("string value mismatch: expected 'zero-copy', got '%s'", strs.Value(2))
	}
}
