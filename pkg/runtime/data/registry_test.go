package data

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestFrameRegistry(t *testing.T) {
	registry := NewFrameRegistry()
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "test", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	rec := b.NewRecord()
	defer rec.Release()

	frame := NewArrowFrame(rec)
	id := "frame-1"

	// Test Registration
	registry.Register(id, frame, nil)
	assert.True(t, registry.Exists(id))

	// Test Reference Counting
	assert.Equal(t, 1, registry.RefCount(id))
	registry.Retain(id)
	assert.Equal(t, 2, registry.RefCount(id))

	registry.Release(id)
	assert.Equal(t, 1, registry.RefCount(id))

	registry.Release(id)
	assert.False(t, registry.Exists(id))
}
