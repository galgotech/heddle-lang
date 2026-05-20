package orchestrator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator"
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
			Output: &schema.FrameSchema{
				Fields: []schema.FrameSchemaField{
					{Name: "col1", ArrowType: "int64"},
				},
			},
		},
		"pkg.to": {
			Input: &schema.FrameSchema{
				Fields: []schema.FrameSchemaField{
					{Name: "col1", ArrowType: "int64"},
				},
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
			Output: &schema.FrameSchema{
				Fields: []schema.FrameSchemaField{
					{Name: "col1", ArrowType: "int64"},
				},
			},
		},
		"pkg.to": {
			Input: &schema.FrameSchema{
				Fields: []schema.FrameSchemaField{
					{Name: "col1", ArrowType: "utf8"},
				},
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
