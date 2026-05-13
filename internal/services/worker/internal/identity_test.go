package internal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

func TestExecuteIdentity(t *testing.T) {
	registry := locality.NewDataLocalityRegistry()
	task := models.StepExecutionTask{
		WorkflowID: "wf-1",
		TaskID:     "task-1",
		Step: &ir.StepInstruction{
			Call: []string{"__internal", "identity"},
		},
	}

	res, err := ExecuteIdentity(context.Background(), task, registry)
	require.NoError(t, err)
	assert.Equal(t, "task-1", res.TaskID)
	assert.Equal(t, models.TaskStatusSuccess, res.Status)
}
