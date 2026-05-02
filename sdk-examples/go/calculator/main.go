package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/galgotech/heddle-lang/sdk/go/core"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type Config struct{}

func getColumns(record arrow.Record) (a, b *array.Float64, err error) {
	schema := record.Schema()

	idxA := schema.FieldIndices("a")
	if len(idxA) == 0 {
		return nil, nil, core.NewBusinessError("column 'a' not found")
	}

	idxB := schema.FieldIndices("b")
	if len(idxB) == 0 {
		return nil, nil, core.NewBusinessError("column 'b' not found")
	}

	colA, ok := record.Column(idxA[0]).(*array.Float64)
	if !ok {
		return nil, nil, core.NewBusinessError("column 'a' must be float64")
	}

	colB, ok := record.Column(idxB[0]).(*array.Float64)
	if !ok {
		return nil, nil, core.NewBusinessError("column 'b' must be float64")
	}

	return colA, colB, nil
}

func createResultTable(results []float64) *core.Table {
	pool := memory.NewGoAllocator()
	builder := array.NewFloat64Builder(pool)
	defer builder.Release()

	builder.AppendValues(results, nil)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	col := builder.NewArray()
	defer col.Release()

	record := array.NewRecord(schema, []arrow.Array{col}, int64(len(results)))
	return core.NewTableFromRecord(record)
}

func Add(ctx context.Context, cfg Config, input *core.Table) (*core.Table, error) {
	colA, colB, err := getColumns(input.Record)
	if err != nil {
		return nil, err
	}

	rows := int(input.Record.NumRows())
	results := make([]float64, rows)
	for i := range rows {
		results[i] = colA.Value(i) + colB.Value(i)
	}

	return createResultTable(results), nil
}

func Subtract(ctx context.Context, cfg Config, input *core.Table) (*core.Table, error) {
	colA, colB, err := getColumns(input.Record)
	if err != nil {
		return nil, err
	}

	rows := int(input.Record.NumRows())
	results := make([]float64, rows)
	for i := 0; i < rows; i++ {
		results[i] = colA.Value(i) - colB.Value(i)
	}

	return createResultTable(results), nil
}

func Multiply(ctx context.Context, cfg Config, input *core.Table) (*core.Table, error) {
	colA, colB, err := getColumns(input.Record)
	if err != nil {
		return nil, err
	}

	rows := int(input.Record.NumRows())
	results := make([]float64, rows)
	for i := 0; i < rows; i++ {
		results[i] = colA.Value(i) * colB.Value(i)
	}

	return createResultTable(results), nil
}

func Divide(ctx context.Context, cfg Config, input *core.Table) (*core.Table, error) {
	colA, colB, err := getColumns(input.Record)
	if err != nil {
		return nil, err
	}

	rows := int(input.Record.NumRows())
	results := make([]float64, rows)
	for i := 0; i < rows; i++ {
		if colB.Value(i) == 0 {
			return nil, core.NewBusinessError(fmt.Sprintf("division by zero at row %d", i))
		}
		results[i] = colA.Value(i) / colB.Value(i)
	}

	return createResultTable(results), nil
}

func main() {
	p := plugin.New()

	plugin.RegisterStep(p, "add", Add)
	plugin.RegisterStep(p, "subtract", Subtract)
	plugin.RegisterStep(p, "multiply", Multiply)
	plugin.RegisterStep(p, "divide", Divide)

	log.Println("Calculator plugin starting...")
	if err := p.Serve(); err != nil {
		log.Fatal(err)
	}
}
