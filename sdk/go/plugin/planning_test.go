package plugin_test

import (
	"encoding/json"
	"testing"

	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

func TestPlugin_PlanningDataHandler(t *testing.T) {
	p := plugin.New("test")

	var receivedData []map[string]any
	p.RegisterPlanningDataHandler(func(data []map[string]any) error {
		receivedData = data
		return nil
	})

	// Simulate receiving a std.data step execution request
	data := []map[string]any{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}
	config := map[string]any{"data": data}
	configJSON, _ := json.Marshal(config)

	req := plugin.ExecuteStepRequest{
		TaskID:     "task_1",
		StepName:   "std.data",
		ConfigJSON: string(configJSON),
	}

	// We need to use reflection to call the internal executeTask method or
	// just test it via the exchange loop if we had a mock worker.
	// Since executeTask is exported in our implementation (wait, is it?),
	// let's check plugin.go.

	// Actually, executeTask is unexported in plugin.go:
	// func (p *Plugin) executeTask(ctx context.Context, req ExecuteStepRequest) ExecuteStepResponse

	// I will temporarily make it exported or use a public entry point if available.
	// Looking at plugin.go, startExecutionLoop calls it.
}
