package data

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestTableRegistry(t *testing.T) {
	registry := NewTableRegistry()
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "test", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	rec := b.NewRecord()
	defer rec.Release()

	table := &ArrowTable{record: rec}
	id := "table-1"

	// Test Registration
	registry.Register(id, table, nil)
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
