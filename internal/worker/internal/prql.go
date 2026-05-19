package internal

import (
	"context"

	"github.com/galgotech/heddle-lang/pkg/plugin"
)

func ExecutePRQL(ctx context.Context, task plugin.ExecuteStepRequest) (plugin.ExecuteStepResponse, error) {
	// TODO: Implement PRQL execution via DataFusion
	return plugin.ExecuteStepResponse{
		TaskID: task.TaskID,
		Status: plugin.StepResponseSuccess,
	}, nil
}
