package internal

import (
	"context"

	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

func ExecuteIdentity(ctx context.Context, task models.StepExecutionTask, registry *locality.DataLocalityRegistry) (models.TaskResult, error) {
	// TODO: Implement identity step
	return models.TaskResult{
		TaskID: task.TaskID,
		Status: models.TaskStatusSuccess,
	}, nil
}
