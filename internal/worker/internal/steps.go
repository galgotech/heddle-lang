package internal

import (
	"context"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

type InternalStepFunc func(ctx context.Context, task models.StepExecutionTask, registry *locality.DataLocalityRegistry) (models.TaskResult, error)

var Registry = map[string]InternalStepFunc{
	"identity":     ExecuteIdentity,
	"prql":         ExecutePRQL,
	"data_literal": ExecuteDataLiteral,
	"compress":     ExecuteCompress,
}
