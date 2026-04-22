package io

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestPrintStep(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "msg", Type: arrow.BinaryTypes.String}},
		nil,
	)

	builder := array.NewStringBuilder(pool)
	builder.Append("Hello Heddle")
	arr := builder.NewArray()
	defer arr.Release()

	batch := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer batch.Release()

	result, err := PrintStep(context.Background(), batch)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.NumRows())
}
