package plugin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlugin_PlanningDataHandler_Internal(t *testing.T) {
	p := New("test")

	var receivedData []map[string]any
	p.RegisterPlanningDataHandler(func(data []map[string]any) error {
		receivedData = data
		return nil
	})

	// Simulate receiving a std.data step execution request
	data := []map[string]any{
		{"id": 1.0, "name": "Alice"}, // JSON numbers are floats by default in Go's map[string]any
		{"id": 2.0, "name": "Bob"},
	}
	config := map[string]any{"data": data}
	configJSON, _ := json.Marshal(config)

	req := ExecuteStepRequest{
		TaskID:     "task_1",
		StepName:   "std.data",
		ConfigJSON: string(configJSON),
	}

	resp := p.executeTask(context.Background(), req)

	assert.Equal(t, "SUCCESS", resp.Status)
	require.Len(t, receivedData, 2)
	assert.Equal(t, 1.0, receivedData[0]["id"])
	assert.Equal(t, "Alice", receivedData[0]["name"])
}
