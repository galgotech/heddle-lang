package main

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/galgotech/heddle-lang/sdk/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestTable(t *testing.T, a, b []float64) *core.Table {
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

	record := array.NewRecord(schema, cols, int64(len(a)))
	return core.NewTableFromRecord(record)
}

func TestAdd(t *testing.T) {
	ctx := context.Background()
	input := createTestTable(t, []float64{10, 20}, []float64{5, 2})
	defer input.Release()

	output, err := Add(ctx, Config{}, input)
	require.NoError(t, err)
	defer output.Release()

	require.NotNil(t, output.Record)
	assert.Equal(t, int64(2), output.Record.NumRows())

	resCol := output.Record.Column(0).(*array.Float64)
	assert.Equal(t, 15.0, resCol.Value(0))
	assert.Equal(t, 22.0, resCol.Value(1))
}

func TestSubtract(t *testing.T) {
	ctx := context.Background()
	input := createTestTable(t, []float64{10, 20}, []float64{5, 2})
	defer input.Release()

	output, err := Subtract(ctx, Config{}, input)
	require.NoError(t, err)
	defer output.Release()

	resCol := output.Record.Column(0).(*array.Float64)
	assert.Equal(t, 5.0, resCol.Value(0))
	assert.Equal(t, 18.0, resCol.Value(1))
}

func TestMultiply(t *testing.T) {
	ctx := context.Background()
	input := createTestTable(t, []float64{10, 20}, []float64{5, 2})
	defer input.Release()

	output, err := Multiply(ctx, Config{}, input)
	require.NoError(t, err)
	defer output.Release()

	resCol := output.Record.Column(0).(*array.Float64)
	assert.Equal(t, 50.0, resCol.Value(0))
	assert.Equal(t, 40.0, resCol.Value(1))
}

func TestDivide(t *testing.T) {
	ctx := context.Background()

	t.Run("Normal division", func(t *testing.T) {
		input := createTestTable(t, []float64{10, 20}, []float64{5, 2})
		defer input.Release()

		output, err := Divide(ctx, Config{}, input)
		require.NoError(t, err)
		defer output.Release()

		resCol := output.Record.Column(0).(*array.Float64)
		assert.Equal(t, 2.0, resCol.Value(0))
		assert.Equal(t, 10.0, resCol.Value(1))
	})

	t.Run("Division by zero", func(t *testing.T) {
		input := createTestTable(t, []float64{10}, []float64{0})
		defer input.Release()

		_, err := Divide(ctx, Config{}, input)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "division by zero")
	})
}

func TestGetColumnsErrors(t *testing.T) {
	ctx := context.Background()
	pool := memory.NewGoAllocator()

	t.Run("Missing column a", func(t *testing.T) {
		builderB := array.NewFloat64Builder(pool)
		defer builderB.Release()
		builderB.AppendValues([]float64{1}, nil)
		schema := arrow.NewSchema([]arrow.Field{{Name: "b", Type: arrow.PrimitiveTypes.Float64}}, nil)
		cols := []arrow.Array{builderB.NewArray()}
		defer cols[0].Release()
		record := array.NewRecord(schema, cols, 1)
		input := core.NewTableFromRecord(record)
		defer input.Release()

		_, err := Add(ctx, Config{}, input)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column 'a' not found")
	})

	t.Run("Missing column b", func(t *testing.T) {
		builderA := array.NewFloat64Builder(pool)
		defer builderA.Release()
		builderA.AppendValues([]float64{1}, nil)
		schema := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Float64}}, nil)
		cols := []arrow.Array{builderA.NewArray()}
		defer cols[0].Release()
		record := array.NewRecord(schema, cols, 1)
		input := core.NewTableFromRecord(record)
		defer input.Release()

		_, err := Add(ctx, Config{}, input)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column 'b' not found")
	})

	t.Run("Wrong type for a", func(t *testing.T) {
		builderA := array.NewInt64Builder(pool)
		defer builderA.Release()
		builderA.AppendValues([]int64{1}, nil)
		builderB := array.NewFloat64Builder(pool)
		defer builderB.Release()
		builderB.AppendValues([]float64{1}, nil)
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			{Name: "b", Type: arrow.PrimitiveTypes.Float64},
		}, nil)
		cols := []arrow.Array{builderA.NewArray(), builderB.NewArray()}
		defer cols[0].Release()
		defer cols[1].Release()
		record := array.NewRecord(schema, cols, 1)
		input := core.NewTableFromRecord(record)
		defer input.Release()

		_, err := Add(ctx, Config{}, input)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column 'a' must be float64")
	})

	t.Run("Wrong type for b", func(t *testing.T) {
		builderA := array.NewFloat64Builder(pool)
		defer builderA.Release()
		builderA.AppendValues([]float64{1}, nil)
		builderB := array.NewInt64Builder(pool)
		defer builderB.Release()
		builderB.AppendValues([]int64{1}, nil)
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Float64},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		cols := []arrow.Array{builderA.NewArray(), builderB.NewArray()}
		defer cols[0].Release()
		defer cols[1].Release()
		record := array.NewRecord(schema, cols, 1)
		input := core.NewTableFromRecord(record)
		defer input.Release()

		_, err := Add(ctx, Config{}, input)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column 'b' must be float64")
	})
}
