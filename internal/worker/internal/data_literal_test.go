package internal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteDataLiteral(t *testing.T) {
	data := []map[string]any{
		{"id": float64(1), "name": "Alice"},
		{"id": float64(2), "name": "Bob"},
	}
	configJSON, err := json.Marshal(map[string]any{
		"literalData": data,
	})
	require.NoError(t, err)

	task := plugin.ExecuteStepRequest{
		WorkflowID: "wf-data-1",
		TaskID:     "task-data-1",
		StepName:   "data_literal",
		ConfigJSON: string(configJSON),
	}

	res, err := ExecuteDataLiteral(context.Background(), task)
	require.NoError(t, err)
	assert.Equal(t, "task-data-1", res.TaskID)
	assert.Equal(t, plugin.StepResponseSuccess, res.Status)

	// Verify OutputHandles are present
	assert.NotEmpty(t, res.OutputHandles["id"])
	assert.NotEmpty(t, res.OutputHandles["name"])

	// Read back from SHM and verify (smoke test)
	arr, err := locality.ReadArrowArrayFromPath(res.OutputHandles["id"])
	require.NoError(t, err)
	assert.Equal(t, 2, arr.Len())
	arr.Release()
}

func TestExecuteDataLiteral_Empty(t *testing.T) {
	data := []map[string]any{}
	configJSON, err := json.Marshal(map[string]any{
		"literalData": data,
	})
	require.NoError(t, err)

	task := plugin.ExecuteStepRequest{
		WorkflowID: "wf-data-empty",
		TaskID:     "task-data-empty",
		StepName:   "data_literal",
		ConfigJSON: string(configJSON),
	}

	res, err := ExecuteDataLiteral(context.Background(), task)
	require.NoError(t, err)
	assert.Equal(t, "task-data-empty", res.TaskID)
	assert.Equal(t, plugin.StepResponseSuccess, res.Status)
	assert.Empty(t, res.OutputHandles)
}

func TestExecuteDataLiteral_Validation(t *testing.T) {
	tests := []struct {
		name       string
		configJSON string
		wantErr    string
	}{
		{
			name:       "Missing config JSON",
			configJSON: "",
			wantErr:    "data_literal: missing step config JSON",
		},
		{
			name:       "Malformed JSON",
			configJSON: "{invalid-json}",
			wantErr:    "data_literal: failed to parse config JSON",
		},
		{
			name:       "Missing literalData key",
			configJSON: `{"otherKey": 123}`,
			wantErr:    "data_literal: missing 'literalData' in step config",
		},
		{
			name:       "literalData not a slice",
			configJSON: `{"literalData": "not-a-slice"}`,
			wantErr:    "data_literal: 'literalData' must be a valid slice of maps",
		},
		{
			name:       "literalData slice element not a map",
			configJSON: `{"literalData": ["not-a-map"]}`,
			wantErr:    "data_literal: element 0 in 'literalData' is not a valid map",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := plugin.ExecuteStepRequest{
				WorkflowID: "wf-val",
				TaskID:     "task-val",
				StepName:   "data_literal",
				ConfigJSON: tt.configJSON,
			}
			res, err := ExecuteDataLiteral(context.Background(), task)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Empty(t, res)
		})
	}
}
