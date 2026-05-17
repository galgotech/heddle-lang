package internal

import (
	"context"
	"testing"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteDataLiteral(t *testing.T) {
	registry := locality.NewDataLocalityRegistry()
	data := []map[string]any{
		{"id": float64(1), "name": "Alice"},
		{"id": float64(2), "name": "Bob"},
	}
	task := models.StepExecutionTask{
		WorkflowID: "wf-data-1",
		TaskID:     "task-data-1",
		Step: &ir.StepInstruction{
			Call:        []string{"__internal", "data_literal"},
			LiteralData: data,
		},
	}

	res, err := ExecuteDataLiteral(context.Background(), task, registry)
	require.NoError(t, err)
	assert.Equal(t, "task-data-1", res.TaskID)
	assert.Equal(t, models.TaskStatusSuccess, res.Status)

	// Verify data in registry
	meta, ok := registry.GetMetadata("wf-data-1", "task-data-1", locality.Output)
	assert.True(t, ok)
	assert.NotEmpty(t, meta.Paths["id"])
	assert.NotEmpty(t, meta.Paths["name"])

	// Read back from SHM and verify (smoke test)
	arr, err := locality.ReadArrowArrayFromPath(meta.Paths["id"])
	require.NoError(t, err)
	assert.Equal(t, 2, arr.Len())
	arr.Release()
}

func TestExecuteDataLiteral_Empty(t *testing.T) {
	registry := locality.NewDataLocalityRegistry()
	data := []map[string]any{}
	task := models.StepExecutionTask{
		WorkflowID: "wf-data-empty",
		TaskID:     "task-data-empty",
		Step: &ir.StepInstruction{
			Call:        []string{"__internal", "data_literal"},
			LiteralData: data,
		},
	}

	res, err := ExecuteDataLiteral(context.Background(), task, registry)
	require.NoError(t, err)
	assert.Equal(t, "task-data-empty", res.TaskID)
	assert.Equal(t, models.TaskStatusSuccess, res.Status)

	// Verify data in registry (should have no paths because schema/record has no columns)
	meta, ok := registry.GetMetadata("wf-data-empty", "task-data-empty", locality.Output)
	assert.True(t, ok)
	assert.Empty(t, meta.Paths)
}
