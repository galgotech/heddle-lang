//go:build !windows

package core_test

import (
	"os"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/galgotech/heddle-lang/sdk/go/core"
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
	path, err := core.WriteArrowToShm(record)
	require.NoError(t, err)
	defer os.Remove(path)

	// 2. Read from path
	readRecord, err := core.ReadArrowFromPath(path)
	require.NoError(t, err)
	defer readRecord.Release()

	assert.Equal(t, record.Schema(), readRecord.Schema())
	assert.Equal(t, record.NumRows(), readRecord.NumRows())
}
