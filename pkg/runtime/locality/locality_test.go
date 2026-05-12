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

	// Create a valid file to Put
	f, err := os.CreateTemp("/dev/shm", "valid-*.arrow")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	f.Chmod(0400)
	f.Close()

	meta := locality.NewMetadata("wf-1", "task-1", locality.Input, map[string]string{"f1": f.Name()})

	// Put should use composite key (WorkflowID + TaskID + IODirection)
	err = r.Put(meta)
	assert.NoError(t, err)

	// Retrieve with correct identifiers
	retrieved, ok := r.GetMetadata("wf-1", "task-1", locality.Input)
	assert.True(t, ok)
	assert.Equal(t, meta, retrieved)

	// Verify not found with wrong identifiers
	_, ok = r.GetMetadata("wf-1", "task-1", locality.Output)
	assert.False(t, ok)
	_, ok = r.GetMetadata("wf-other", "task-1", locality.Input)
	assert.False(t, ok)

	// Delete
	r.Delete("wf-1", "task-1", locality.Input)
	_, ok = r.GetMetadata("wf-1", "task-1", locality.Input)
	assert.False(t, ok)
}

func TestDeleteByWorkflow(t *testing.T) {
	r := locality.NewDataLocalityRegistry()

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

	r.Put(locality.NewMetadata("wf-1", "task-1", locality.Output, map[string]string{"f1": f1.Name()}))
	r.Put(locality.NewMetadata("wf-1", "task-2", locality.Output, map[string]string{"f2": f2.Name()}))
	r.Put(locality.NewMetadata("wf-2", "task-1", locality.Output, map[string]string{"f3": f3.Name()}))

	// Delete wf-1
	r.DeleteByWorkflow("wf-1")

	// wf-1 entries should be gone
	_, ok := r.GetMetadata("wf-1", "task-1", locality.Output)
	assert.False(t, ok)
	_, ok = r.GetMetadata("wf-1", "task-2", locality.Output)
	assert.False(t, ok)

	// wf-1 files should be gone
	_, err := os.Stat(f1.Name())
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(f2.Name())
	assert.True(t, os.IsNotExist(err))

	// wf-2 entry and file should still exist
	meta3, ok := r.GetMetadata("wf-2", "task-1", locality.Output)
	assert.True(t, ok)
	assert.Equal(t, "wf-2", meta3.WorkflowID)
	_, err = os.Stat(f3.Name())
	assert.NoError(t, err)
}

func TestAllocateAndWrite_Permissions(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Float64}}, nil)
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	b.Append(1.0)
	arr := b.NewArray()
	defer arr.Release()
	record := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer record.Release()

	f, err := locality.AllocateAndWrite(record)
	require.NoError(t, err)
	defer locality.Unlink(f)

	info, err := f.Stat()
	require.NoError(t, err)
	// After seal, should be 0400 (read-only for owner)
	assert.Equal(t, os.FileMode(0400), info.Mode().Perm())
}

func TestReadFromPath_RejectsInsecureFile(t *testing.T) {
	// Create a world-readable file
	f, err := os.CreateTemp("/dev/shm", "insecure-*.arrow")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	f.Close()

	err = os.Chmod(f.Name(), 0644)
	require.NoError(t, err)

	_, err = locality.ReadFromPath(f.Name())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insecure permissions")
}

func TestRegistry_Put_RejectsInsecurePath(t *testing.T) {
	r := locality.NewDataLocalityRegistry()

	// Create a world-readable file
	f, err := os.CreateTemp("/dev/shm", "insecure-reg-*.arrow")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	f.Close()
	err = os.Chmod(f.Name(), 0644)
	require.NoError(t, err)

	meta := locality.NewMetadata("wf-fail", "task-fail", locality.Output, map[string]string{"f1": f.Name()})
	err = r.Put(meta)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insecure permissions")
}
