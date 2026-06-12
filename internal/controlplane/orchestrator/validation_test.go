package orchestrator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/internal/controlplane/orchestrator"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

func TestValidateEdge_EmptyFromID(t *testing.T) {
	prog := ir.Program{}

	err := orchestrator.ValidateEdge(prog, "", "step-1", nil)
	assert.NoError(t, err)
}

func TestValidateEdge_Compatible(t *testing.T) {
	schemas := map[string]schema.StepSchemas{
		"pkg.from": {
			Output: []schema.ColumnSchema{
				{Name: "col1", ArrowType: "int64"},
			},
		},
		"pkg.to": {
			Input: []schema.ColumnSchema{
				{Name: "col1", ArrowType: "int64"},
			},
		},
	}

	prog := ir.Program{
		Instructions: map[string]any{
			"step-1": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-1"},
				Call:            []string{"pkg", "from"},
			},
			"step-2": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-2"},
				Call:            []string{"pkg", "to"},
			},
		},
	}

	err := orchestrator.ValidateEdge(prog, "step-1", "step-2", schemas)
	assert.NoError(t, err)
}

func TestValidateEdge_Incompatible(t *testing.T) {
	schemas := map[string]schema.StepSchemas{
		"pkg.from": {
			Output: []schema.ColumnSchema{
				{Name: "col1", ArrowType: "int64"},
			},
		},
		"pkg.to": {
			Input: []schema.ColumnSchema{
				{Name: "col1", ArrowType: "utf8"},
			},
		},
	}

	prog := ir.Program{
		Instructions: map[string]any{
			"step-1": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-1"},
				Call:            []string{"pkg", "from"},
			},
			"step-2": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-2"},
				Call:            []string{"pkg", "to"},
			},
		},
	}

	err := orchestrator.ValidateEdge(prog, "step-1", "step-2", schemas)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DAG Type Error: pkg.from -> pkg.to")
}

func TestValidateEdge_DynamicCastAndPrint(t *testing.T) {
	schemas := map[string]schema.StepSchemas{
		"pkg.from": {
			Output: []schema.ColumnSchema{
				{Name: "col1", ArrowType: "int64"},
			},
		},
	}

	prog := ir.Program{
		Instructions: map[string]any{
			"step-1": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-1"},
				Call:            []string{"pkg", "from"},
			},
			"step-2": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-2"},
				Call:            []string{"std/col", "cast"},
				Config:          map[string]any{"to": "string"},
				Parents:         []string{"step-1"},
			},
			"step-3": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-3"},
				Call:            []string{"std/io", "print"},
				Parents:         []string{"step-2"},
			},
		},
	}

	// Validate step-1 -> step-2 (pkg.from -> std/col.cast)
	err := orchestrator.ValidateEdge(prog, "step-1", "step-2", schemas)
	assert.NoError(t, err)

	// Validate step-2 -> step-3 (std/col.cast -> std/io.print)
	err = orchestrator.ValidateEdge(prog, "step-2", "step-3", schemas)
	assert.NoError(t, err)
}

func TestValidateEdge_DynamicCastColumns(t *testing.T) {
	schemas := map[string]schema.StepSchemas{
		"pkg.from": {
			Output: []schema.ColumnSchema{
				{Name: "col1", ArrowType: "int64"},
				{Name: "col2", ArrowType: "utf8"},
			},
		},
	}

	prog := ir.Program{
		Instructions: map[string]any{
			"step-1": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-1"},
				Call:            []string{"pkg", "from"},
			},
			"step-2": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-2"},
				Call:            []string{"std/col", "cast"},
				Config:          map[string]any{"columns": map[string]any{"col1": "string"}},
				Parents:         []string{"step-1"},
			},
		},
	}

	err := orchestrator.ValidateEdge(prog, "step-1", "step-2", schemas)
	assert.NoError(t, err)
}

func TestValidateEdge_DynamicCastFailure(t *testing.T) {
	schemas := map[string]schema.StepSchemas{
		"pkg.from": {
			Output: []schema.ColumnSchema{
				{Name: "col1", ArrowType: "int64"},
			},
		},
	}

	prog := ir.Program{
		Instructions: map[string]any{
			"step-1": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-1"},
				Call:            []string{"pkg", "from"},
			},
			"step-2": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-2"},
				Call:            []string{"std/col", "cast"},
				Config:          map[string]any{"to": "invalid_type"},
				Parents:         []string{"step-1"},
			},
		},
	}

	err := orchestrator.ValidateEdge(prog, "step-1", "step-2", schemas)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cast: invalid target type")
}

func TestValidateEdge_DynamicPrintFailure(t *testing.T) {
	schemas := map[string]schema.StepSchemas{
		"pkg.from": {
			Output: []schema.ColumnSchema{
				{Name: "col1", ArrowType: "struct"},
			},
		},
	}

	prog := ir.Program{
		Instructions: map[string]any{
			"step-1": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-1"},
				Call:            []string{"pkg", "from"},
			},
			"step-2": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-2"},
				Call:            []string{"std/io", "print"},
				Parents:         []string{"step-1"},
			},
		},
	}

	err := orchestrator.ValidateEdge(prog, "step-1", "step-2", schemas)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "print: column 'col1' has unprintable type 'struct'")
}

func TestValidateEdge_DynamicCastMissingConfig(t *testing.T) {
	schemas := map[string]schema.StepSchemas{
		"pkg.from": {
			Output: []schema.ColumnSchema{
				{Name: "col1", ArrowType: "int64"},
			},
		},
	}

	prog := ir.Program{
		Instructions: map[string]any{
			"step-1": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-1"},
				Call:            []string{"pkg", "from"},
			},
			"step-2": ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{ID: "step-2"},
				Call:            []string{"std/col", "cast"},
				Config:          map[string]any{},
				Parents:         []string{"step-1"},
			},
		},
	}

	err := orchestrator.ValidateEdge(prog, "step-1", "step-2", schemas)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cast: config must specify either 'columns' map or global 'to' type")
}

