package std

import (
	"context"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

// Registry maps stdlib namespaces to their internal step implementations.
// Key format: "std/namespace:step" (e.g., "std/io:print")

type StdStepFunc func(ctx context.Context, task models.StepExecutionTask, registry *locality.DataLocalityRegistry) (models.TaskResult, error)

var Registry = map[string]StdStepFunc{
	"std/io:print": ExecutePrint,
}
