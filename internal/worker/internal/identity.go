package internal

import (
	"context"

	"github.com/galgotech/heddle-lang/pkg/plugin"
)

func ExecuteIdentity(ctx context.Context, task plugin.ExecuteStepRequest) (plugin.ExecuteStepResponse, error) {
	// TODO: Implement identity step
	return plugin.ExecuteStepResponse{
		TaskID: task.TaskID,
		Status: plugin.StepResponseSuccess,
	}, nil
}
