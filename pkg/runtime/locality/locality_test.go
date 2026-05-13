package locality

import (
	"os"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDKLocalityZeroCopy(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	b := array.NewFloat64Builder(mem)
	defer b.Release()
	b.Append(42.0)

	arr := b.NewArray()
	defer arr.Release()

	record := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer record.Release()

	// 1. Write to SHM
	path, err := WriteArrowArrayOnlyToShm(arr)
	require.NoError(t, err)
	defer os.Remove(path)

	// 2. Read from path
	readRecord, err := ReadArrowArrayFromPath(path)
	require.NoError(t, err)
	defer readRecord.Release()
}

func TestDataLocalityRegistry(t *testing.T) {
	r := NewDataLocalityRegistry()

	// Create a valid file to Put
	f, err := os.CreateTemp("/dev/shm", "valid-*.arrow")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	f.Chmod(0400)
	f.Close()

	meta := NewMetadata("wf-1", "task-1", Input, map[string]string{"f1": f.Name()})

	// Put should use composite key (WorkflowID + TaskID + IODirection)
	err = r.Put(meta)
	assert.NoError(t, err)

	// Retrieve with correct identifiers
	retrieved, ok := r.GetMetadata("wf-1", "task-1", Input)
	assert.True(t, ok)
	assert.Equal(t, meta, retrieved)

	// Verify not found with wrong identifiers
	_, ok = r.GetMetadata("wf-1", "task-1", Output)
	assert.False(t, ok)
	_, ok = r.GetMetadata("wf-other", "task-1", Input)
	assert.False(t, ok)

	// Delete
	r.Delete("wf-1", "task-1", Input)
	_, ok = r.GetMetadata("wf-1", "task-1", Input)
	assert.False(t, ok)
}

func TestDeleteByWorkflow(t *testing.T) {
	r := NewDataLocalityRegistry()

	// Create 3 valid files
	f1, _ := os.CreateTemp("/dev/shm", "wf1-task1-*.arrow")
	f1.Chmod(0400)
	f1.Close()
	defer os.Remove(f1.Name())

	f2, _ := os.CreateTemp("/dev/shm", "wf1-task2-*.arrow")
	f2.Chmod(0400)
	f2.Close()
	defer os.Remove(f2.Name())

	f3, _ := os.CreateTemp("/dev/shm", "wf2-task1-*.arrow")
	f3.Chmod(0400)
	f3.Close()
	defer os.Remove(f3.Name())

	r.Put(NewMetadata("wf-1", "task-1", Output, map[string]string{"f1": f1.Name()}))
	r.Put(NewMetadata("wf-1", "task-2", Output, map[string]string{"f2": f2.Name()}))
	r.Put(NewMetadata("wf-2", "task-1", Output, map[string]string{"f3": f3.Name()}))

	// Delete wf-1
	r.DeleteByWorkflow("wf-1")

	// wf-1 entries should be gone
	_, ok := r.GetMetadata("wf-1", "task-1", Output)
	assert.False(t, ok)
	_, ok = r.GetMetadata("wf-1", "task-2", Output)
	assert.False(t, ok)

	// wf-1 files should be gone
	_, err := os.Stat(f1.Name())
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(f2.Name())
	assert.True(t, os.IsNotExist(err))

	// wf-2 entry and file should still exist
	meta3, ok := r.GetMetadata("wf-2", "task-1", Output)
	assert.True(t, ok)
	assert.Equal(t, "wf-2", meta3.WorkflowID)
	_, err = os.Stat(f3.Name())
	assert.NoError(t, err)
}

func TestRegistry_Put_RejectsInsecurePath(t *testing.T) {
	r := NewDataLocalityRegistry()

	// Create a world-readable file
	f, err := os.CreateTemp("/dev/shm", "insecure-reg-*.arrow")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	f.Close()
	err = os.Chmod(f.Name(), 0644)
	require.NoError(t, err)

	meta := NewMetadata("wf-fail", "task-fail", Output, map[string]string{"f1": f.Name()})
	err = r.Put(meta)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insecure permissions")
}
