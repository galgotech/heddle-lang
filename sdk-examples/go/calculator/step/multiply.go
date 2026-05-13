package step

import (
	"context"

	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type MultiplyConfig struct {
	plugin.Config

	Multiplier float64 `heddle:"multiplier"`
}

type MultiplyFrame struct {
	plugin.HeddleFrame

	A *plugin.Float64
}

func Multiply(ctx context.Context, cfg MultiplyConfig, input *MultiplyFrame, output *ResultTable) error {
	return input.Multiply(ctx, input.A, cfg.Multiplier, output.A)
}
