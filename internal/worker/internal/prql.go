package internal

import (
	"context"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

func ExecutePRQL(ctx context.Context, task models.StepExecutionTask, registry *locality.DataLocalityRegistry) (models.TaskResult, error) {
	// TODO: Implement PRQL execution via DataFusion
	return models.TaskResult{
		TaskID: task.TaskID,
		Status: models.TaskStatusSuccess,
	}, nil
}
