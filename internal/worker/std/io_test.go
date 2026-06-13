package std

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/plugin"
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

func TestExecuteLoadCSV(t *testing.T) {
	cfgJSON := `{
		"path": "test_data.csv",
		"delimiter": ";",
		"lazy_quotes": true,
		"columns": {
			"id": "int64",
			"name": "string"
		}
	}`

	var sentMessage *models.ControlMessage
	mockSender := func(msg *models.ControlMessage) error {
		sentMessage = msg
		return nil
	}

	mockWaiter := func(ctx context.Context, taskID string) (map[string]string, error) {
		assert.Equal(t, "task-123", taskID)
		return map[string]string{"data": "/dev/shm/test_data_handle"}, nil
	}

	ctx := context.Background()
	ctx = models.WithControlSender(ctx, mockSender)
	ctx = models.WithUploadWaiter(ctx, mockWaiter)

	req := plugin.ExecuteStepRequest{
		WorkflowID: "wf-123",
		TaskID:     "task-123",
		StepName:   "load_csv",
		ConfigJSON: cfgJSON,
	}

	res, err := ExecuteLoadCSV(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, plugin.StepResponseSuccess, res.Status)
	assert.Equal(t, "/dev/shm/test_data_handle", res.OutputRef["data"])

	// Verify the request details sent via the ControlSender
	require.NotNil(t, sentMessage)
	assert.Equal(t, models.ActionRequestFile, sentMessage.Type)
	require.NotNil(t, sentMessage.FileRequest)
	assert.Equal(t, "test_data.csv", sentMessage.FileRequest.FilePath)
	assert.Equal(t, "wf-123", sentMessage.FileRequest.WorkflowID)
	assert.Equal(t, "task-123", sentMessage.FileRequest.TaskID)

	// Assertions for parsed CSV options and columns
	assert.Equal(t, ";", sentMessage.FileRequest.Options["delimiter"])
	assert.Equal(t, true, sentMessage.FileRequest.Options["lazy_quotes"])
	assert.Equal(t, "int64", sentMessage.FileRequest.Columns["id"])
	assert.Equal(t, "string", sentMessage.FileRequest.Columns["name"])
}
