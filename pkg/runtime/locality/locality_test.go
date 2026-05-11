package locality_test

import (
	"os"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalityZeroCopy(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f1", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	b := array.NewFloat64Builder(mem)
	defer b.Release()
	b.Append(1.0)
	b.Append(2.0)
	b.Append(3.0)

	arr := b.NewArray()
	defer arr.Release()

	record := array.NewRecord(schema, []arrow.Array{arr}, 3)
	defer record.Release()

	// 1. Allocate and Write
	f, err := locality.AllocateAndWrite(record)
	require.NoError(t, err)
	path := f.Name()

	// 2. Read from path (mmap)
	readRecord, err := locality.ReadFromPath(path)
	require.NoError(t, err)
	defer readRecord.Release()

	assert.Equal(t, record.Schema(), readRecord.Schema())
	assert.Equal(t, record.NumRows(), readRecord.NumRows())

	// 3. Unlink
	err = locality.Unlink(f)
	assert.NoError(t, err)

	// Verify file is gone
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func TestDataLocalityRegistry(t *testing.T) {
	r := locality.NewDataLocalityRegistry()

	meta := locality.Metadata{
		TaskID:      "task-1",
		IODirection: locality.Input,
		Path:        "/dev/shm/test",
	}

	// Put should use composite key (TaskID + IODirection)
	r.Put(meta)

	// Retrieve with correct direction
	retrieved, ok := r.GetMetadata("task-1", locality.Input)
	assert.True(t, ok)
	assert.Equal(t, meta, retrieved)

	// Verify not found with wrong direction
	_, ok = r.GetMetadata("task-1", locality.Output)
	assert.False(t, ok)

	// Delete
	r.Delete("task-1", locality.Input)
	_, ok = r.GetMetadata("task-1", locality.Input)
	assert.False(t, ok)
}
