package step

import (
	"context"

	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type SubConfig struct {
	plugin.Config
}

type SubFrame struct {
	plugin.HeddleFrame

	A *plugin.Float64
	B *plugin.Float64
}

func Subtract(ctx context.Context, cfg SubConfig, input *SubFrame, output *ResultTable) error {
	return input.Subtract(ctx, input.A, input.B, output.A)
}
