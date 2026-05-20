package compiler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
)

func TestCompiler_SourceLocations(t *testing.T) {
	input := `import "std/io" io

step log_msg = io.log {
  msg: "hello"
}

workflow main {
  log_msg
}
`
	c := New()
	irProg, err := c.Compile(input)
	require.NoError(t, err)
	require.NotNil(t, irProg)

	for id, inst := range irProg.Instructions {
		if s, ok := inst.(ir.StepInstruction); ok {
			t.Logf("Step ID: %s, DefName: %s, Location: %+v", id, s.DefinitionName, s.SourceLocation)
		}
	}

	var defStep, callStep ir.StepInstruction
	for id, inst := range irProg.Instructions {
		if s, ok := inst.(ir.StepInstruction); ok && s.DefinitionName == "log_msg" {
			if id == "step_1" {
				defStep = s
			} else if id == "step_call_2" {
				callStep = s
			}
		}
	}

	require.NotNil(t, defStep, "Definition step 'step_1' not found")
	require.NotNil(t, defStep.SourceLocation, "defStep.SourceLocation is nil")
	assert.Equal(t, 3, defStep.SourceLocation.Line)

	require.NotNil(t, callStep, "Call step 'step_call_2' not found")
	require.NotNil(t, callStep.SourceLocation, "callStep.SourceLocation is nil")
	assert.Equal(t, 8, callStep.SourceLocation.Line)
}
