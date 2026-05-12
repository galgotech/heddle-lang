package main

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type Config struct {
	plugin.Config
}

type InputFrame struct {
	plugin.HeddleFrame

	A plugin.Field[float64] `heddle:"a"`
	B plugin.Field[float64] `heddle:"b"`
}

type ResultTable struct {
	plugin.HeddleFrame

	A plugin.Field[float64] `heddle:"a"`
}

func Add(ctx context.Context, cfg Config, input InputFrame) (ResultTable, error) {
	rows := input.NumRows()
	results := make([]float64, rows)
	for i := range rows {
		results[i] = input.A.Value(i) + input.B.Value(i)
	}

	res := ResultTable{}
	res.A.SetValues(results)
	return res, nil
}

func Subtract(ctx context.Context, cfg Config, input InputFrame) (ResultTable, error) {
	rows := input.NumRows()
	results := make([]float64, rows)
	for i := range rows {
		results[i] = input.A.Value(i) - input.B.Value(i)
	}

	res := ResultTable{}
	res.A.SetValues(results)
	return res, nil
}

func Multiply(ctx context.Context, cfg Config, input InputFrame) (ResultTable, error) {
	rows := input.NumRows()
	results := make([]float64, rows)
	for i := range rows {
		results[i] = input.A.Value(i) * input.B.Value(i)
	}

	res := ResultTable{}
	res.A.SetValues(results)
	return res, nil
}

func Divide(ctx context.Context, cfg Config, input InputFrame) (ResultTable, error) {
	rows := input.NumRows()
	results := make([]float64, rows)
	for i := range rows {
		if input.B.Value(i) == 0 {
			return ResultTable{}, fmt.Errorf("division by zero at row %d", i)
		}
		results[i] = input.A.Value(i) / input.B.Value(i)
	}

	res := ResultTable{}
	res.A.SetValues(results)
	return res, nil
}

func main() {
	p := plugin.New("calculator")

	p.RegisterStep("add", Add)
	p.RegisterStep("subtract", Subtract)
	p.RegisterStep("multiply", Multiply)
	p.RegisterStep("divide", Divide)

	logger.L().Info("Calculator plugin starting...")
	if err := p.Start(); err != nil {
		logger.L().Fatal("Plugin failed", zap.Error(err))
	}
}
