package data

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataManager_Import(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "heddle-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	mgr, err := NewDataManager(tmpDir, 1<<30)
	require.NoError(t, err)
	defer mgr.Cleanup()

	// 1. Create a record and write it to a file manually (simulating a plugin)
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{42, 43}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	handle := "ext-handle-1"
	path := filepath.Join(tmpDir, handle)
	f, err := os.Create(path)
	require.NoError(t, err)

	writer := ipc.NewWriter(f, ipc.WithSchema(schema))
	err = writer.Write(rec)
	require.NoError(t, err)
	writer.Close()
	f.Close()

	// 2. Import the handle
	err = mgr.Import(handle)
	require.NoError(t, err)

	// 3. Verify it exists and data is correct
	gotRec, err := mgr.Get(handle)
	require.NoError(t, err)
	defer gotRec.Release()

	assert.Equal(t, int64(2), gotRec.NumRows())
	col := gotRec.Column(0).(*array.Int32)
	assert.Equal(t, int32(42), col.Value(0))
	assert.Equal(t, int32(43), col.Value(1))
}
