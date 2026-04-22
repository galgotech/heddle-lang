package execution

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

// IncrementStep is a sample imperative step that increments the first column of a RecordBatch.
func IncrementStep(ctx context.Context, input arrow.Record) (arrow.Record, error) {
	if input == nil || input.NumCols() == 0 {
		return nil, fmt.Errorf("invalid input")
	}

	col := input.Column(0)
	if col.DataType().ID() != arrow.INT64 {
		return nil, fmt.Errorf("expected INT64 column, got %s", col.DataType().Name())
	}

	intArray := col.(*array.Int64)
	pool := memory.NewGoAllocator()
	builder := array.NewInt64Builder(pool)
	defer builder.Release()

	for i := 0; i < intArray.Len(); i++ {
		if intArray.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(intArray.Value(i) + 1)
		}
	}

	newCol := builder.NewArray()
	defer newCol.Release()

	schema := input.Schema()
	return array.NewRecord(schema, []arrow.Array{newCol}, input.NumRows()), nil
}

func init() {
	GlobalRegistry.Register("test", "increment", IncrementStep)
}
