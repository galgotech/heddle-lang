package step

import (
	"context"
	"fmt"

	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type AddConfig struct {
	plugin.Config
}

type AddFrame struct {
	plugin.HeddleFrame

	A *plugin.Float64
	B *plugin.Float64
}

type ResultTable struct {
	plugin.HeddleFrame

	A *plugin.Float64
}

func Add(ctx context.Context, cfg AddConfig, input *AddFrame, output *ResultTable) error {
	a := input.A.Value(0)
	b := input.B.Value(0)
	err := input.Add(ctx, input.A, input.B, output.A)
	if err != nil {
		return err
	}
	c := output.A.Value(0)
	fmt.Println(a, b, c)
	return nil
}
