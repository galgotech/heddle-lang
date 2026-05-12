package plugin_test

import (
	"context"
	"testing"

	"github.com/galgotech/heddle-lang/sdk/go/plugin"
	"github.com/stretchr/testify/require"
)

type TestConfig struct {
	plugin.Config
}

type TestInput struct {
	plugin.HeddleFrame
	A *plugin.Int64 `heddle:"a"`
}

type TestOutput struct {
	plugin.HeddleFrame
	B *plugin.Int64 `heddle:"b"`
}

func StepNewSignature(ctx context.Context, cfg TestConfig, input TestInput, output *TestOutput) error {
	output.B = plugin.NewInt64([]int64{1, 2, 3})
	return nil
}

func TestRegisterStep_NewSignature(t *testing.T) {
	p := plugin.New("test")
	err := p.RegisterStep("test_step", StepNewSignature)
	require.NoError(t, err)
}
