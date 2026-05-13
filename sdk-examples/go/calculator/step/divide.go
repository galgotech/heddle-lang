package step

import (
	"context"

	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type DivideConfig struct {
	plugin.Config

	Divisor float64 `heddle:"divisor"`
}

type DivideFrame struct {
	plugin.HeddleFrame

	A *plugin.Float64
}

func Divide(ctx context.Context, cfg DivideConfig, input *DivideFrame, output *ResultTable) error {
	return input.Divide(ctx, input.A, cfg.Divisor, output.A)
}
