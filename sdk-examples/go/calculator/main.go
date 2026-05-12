package main

import (
	"context"

	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type Config struct {
	plugin.Config
}

type InputFrame struct {
	plugin.HeddleFrame

	A plugin.Float64
	B plugin.Float64
}

type ResultTable struct {
	plugin.HeddleFrame

	A plugin.Float64
}

func Add(ctx context.Context, cfg Config, input InputFrame, output ResultTable) error {
	return input.Add(ctx, &input.A, &input.B, &output.A)
}

func Subtract(ctx context.Context, cfg Config, input InputFrame, output ResultTable) error {
	return input.Subtract(ctx, &input.A, &input.B, &output.A)
}

func Multiply(ctx context.Context, cfg Config, input InputFrame, output ResultTable) error {
	return nil
}

func Divide(ctx context.Context, cfg Config, input InputFrame, output ResultTable) error {
	return nil
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
