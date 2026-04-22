package execution

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestRegistry(t *testing.T) {
	reg := NewRegistry()

	testFunc := func(ctx context.Context, input arrow.Record) (arrow.Record, error) {
		return input, nil
	}

	reg.Register("math", "add", testFunc)

	fn, ok := reg.Get("math", "add")
	assert.True(t, ok)
	assert.NotNil(t, fn)

	_, ok = reg.Get("math", "sub")
	assert.False(t, ok)
}

func TestIncrementStep(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "val", Type: arrow.PrimitiveTypes.Int64}},
		nil,
	)

	builder := array.NewInt64Builder(pool)
	builder.Append(10)
	builder.Append(20)
	arr := builder.NewArray()
	defer arr.Release()

	batch := array.NewRecord(schema, []arrow.Array{arr}, 2)
	defer batch.Release()

	result, err := IncrementStep(context.Background(), batch)
	assert.NoError(t, err)
	defer result.Release()

	assert.Equal(t, int64(2), result.NumRows())
	resCol := result.Column(0).(*array.Int64)
	assert.Equal(t, int64(11), resCol.Value(0))
	assert.Equal(t, int64(21), resCol.Value(1))
}
