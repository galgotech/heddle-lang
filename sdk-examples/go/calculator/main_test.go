package main

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

func createTestInput(t *testing.T, a, b []float64) InputFrame {
	pool := memory.NewGoAllocator()

	builderA := array.NewFloat64Builder(pool)
	defer builderA.Release()
	builderA.AppendValues(a, nil)

	builderB := array.NewFloat64Builder(pool)
	defer builderB.Release()
	builderB.AppendValues(b, nil)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Float64},
		{Name: "b", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	cols := []arrow.Array{builderA.NewArray(), builderB.NewArray()}
	defer cols[0].Release()
	defer cols[1].Release()

	table := array.NewTableFromRecords(schema, []arrow.Record{
		array.NewRecord(schema, cols, int64(len(a))),
	})

	input := InputFrame{}
	err := plugin.Bind(&input, table)
	require.NoError(t, err)

	return input
}

func TestAdd(t *testing.T) {
	ctx := context.Background()
	input := createTestInput(t, []float64{10, 20}, []float64{5, 2})

	output, err := Add(ctx, Config{}, input)
	require.NoError(t, err)

	assert.Equal(t, 2, output.A.Len())
	assert.Equal(t, 15.0, output.A.Value(0))
	assert.Equal(t, 22.0, output.A.Value(1))
}

func TestSubtract(t *testing.T) {
	ctx := context.Background()
	input := createTestInput(t, []float64{10, 20}, []float64{5, 2})

	output, err := Subtract(ctx, Config{}, input)
	require.NoError(t, err)

	assert.Equal(t, 5.0, output.A.Value(0))
	assert.Equal(t, 18.0, output.A.Value(1))
}

func TestMultiply(t *testing.T) {
	ctx := context.Background()
	input := createTestInput(t, []float64{10, 20}, []float64{5, 2})

	output, err := Multiply(ctx, Config{}, input)
	require.NoError(t, err)

	assert.Equal(t, 50.0, output.A.Value(0))
	assert.Equal(t, 40.0, output.A.Value(1))
}

func TestDivide(t *testing.T) {
	ctx := context.Background()

	t.Run("Normal division", func(t *testing.T) {
		input := createTestInput(t, []float64{10, 20}, []float64{5, 2})

		output, err := Divide(ctx, Config{}, input)
		require.NoError(t, err)

		assert.Equal(t, 2.0, output.A.Value(0))
		assert.Equal(t, 10.0, output.A.Value(1))
	})

	t.Run("Division by zero", func(t *testing.T) {
		input := createTestInput(t, []float64{10}, []float64{0})

		_, err := Divide(ctx, Config{}, input)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "division by zero")
	})
}
