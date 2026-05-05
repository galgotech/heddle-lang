package data

import (
	"os"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataManager_PutMmap(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "heddle-mmap-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	mgr, err := NewLocalMmapManager(tmpDir, 0)
	require.NoError(t, err)
	defer mgr.Cleanup()

	// 1. Create a large record to ensure alignment and multiple buffers
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int", Type: arrow.PrimitiveTypes.Int64},
		{Name: "str", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "bb", "ccc", "dddd", "eeeee"}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	// 2. Put into manager
	id := "mmap-test-1"
	err = mgr.Put(id, rec)
	require.NoError(t, err)

	// 3. Get file and verify size
	f := mgr.GetRegistry().GetFile(id)
	require.NotNil(t, f)

	fi, err := f.Stat()
	require.NoError(t, err)
	assert.Greater(t, fi.Size(), int64(0))

	// 4. Get record back and verify integrity
	rec2, err := mgr.Get(id)
	require.NoError(t, err)
	defer rec2.Release()

	assert.Equal(t, rec.NumRows(), rec2.NumRows())
	assert.Equal(t, rec.Schema().String(), rec2.Schema().String())

	// Check values
	assert.Equal(t, int64(1), rec2.Column(0).(*array.Int64).Value(0))
	assert.Equal(t, "eeeee", rec2.Column(1).(*array.String).Value(4))
}
