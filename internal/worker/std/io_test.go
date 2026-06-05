package std

import (
	"bytes"
	"context"
	"testing"

	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutePrint_WithRedirectedOutput(t *testing.T) {
	// Create context with bytes.Buffer writer
	buf := new(bytes.Buffer)
	ctx := plugin.WithOutputWriter(context.Background(), buf)

	req := plugin.ExecuteStepRequest{
		WorkflowID: "wf-print",
		TaskID:     "task-print",
		StepName:   "print",
		InputRef:   map[string]string{},
	}

	res, err := ExecutePrint(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "task-print", res.TaskID)
	assert.Equal(t, plugin.StepResponseSuccess, res.Status)

	// Verify that print boundaries were written to the buffer
	output := buf.String()
	assert.Contains(t, output, "--- std/io.print ---")
	assert.Contains(t, output, "--------------------")
}
