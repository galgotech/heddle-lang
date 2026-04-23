package data

import (
	"os"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestDataManagerSpill(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "heddle-spill-*")
	defer os.RemoveAll(tempDir)

	// Create manager with very low memory limit to force spill
	manager := NewDataManager(tempDir, 1024) // 1KB limit

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "data", Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	// Add enough data to exceed 1KB
	largeString := make([]byte, 2048)
	for i := range largeString {
		largeString[i] = 'A'
	}
	b.Field(0).(*array.StringBuilder).Append(string(largeString))
	rec := b.NewRecord()
	defer rec.Release()

	id := "spill-frame"
	err := manager.Put(id, rec)
	assert.NoError(t, err)

	// Check if frame exists in registry
	frame := manager.GetRegistry().Get(id)
	assert.NotNil(t, frame)
	
	// Since we exceeded 1KB, it should be on disk
	assert.Equal(t, LocationDisk, frame.Location())

	// Verify we can read it back correctly
	rec2, err := manager.Get(id)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rec2.NumRows())
	rec2.Release()
}
