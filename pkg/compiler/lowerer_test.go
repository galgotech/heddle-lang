package compiler

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/ir"
)

func testCode(t *testing.T, code string) *ir.ProgramIR {
	c := New()
	program, err := c.Compile(code)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	return program
}

func TestLowererBasic(t *testing.T) {
	code := `import "fhub/etl" etl

resource db = etl.connect {
  host: "localhost"
}

step extract: void -> user = etl.extract
step transform: user -> user = etl.transform

workflow main {
  extract
    | transform
  > data
}
`

	program := testCode(t, code)

	if len(program.Workflows) != 1 {
		t.Errorf("expected 1 workflow, got %d", len(program.Workflows))
	}

	flowID := program.Workflows[0]
	flow := program.Instructions[flowID].(*ir.FlowInstruction)

	if flow.Name != "main" {
		t.Errorf("expected workflow name 'main', got %s", flow.Name)
	}

	if len(flow.Heads) != 1 {
		t.Errorf("expected 1 flow head, got %d", len(flow.Heads))
	}

	headID := flow.Heads[0]
	step := program.Instructions[headID].(*ir.StepInstruction)

	if step.DefinitionName != "extract" {
		t.Errorf("expected first step 'extract', got %s", step.DefinitionName)
	}

	if step.Next == "" {
		t.Error("expected first step to have a 'next' step")
	}

	nextStep := program.Instructions[step.Next].(*ir.StepInstruction)
	if nextStep.DefinitionName != "transform" {
		t.Errorf("expected next step 'transform', got %s", nextStep.DefinitionName)
	}

	if nextStep.Assignment != "data" {
		t.Errorf("expected assignment 'data', got %s", nextStep.Assignment)
	}
}

func TestLowererWithHandlers(t *testing.T) {
	code := `import "fhub/etl" etl

step extract: void -> user = etl.extract
step retry: void -> user = etl.retry

handler recover {
  * retry
}

workflow main {
  extract ? recover
}
`

	program := testCode(t, code)

	flowID := program.Workflows[0]
	flow := program.Instructions[flowID].(*ir.FlowInstruction)
	headID := flow.Heads[0]
	step := program.Instructions[headID].(*ir.StepInstruction)

	if step.Handler == "" {
		t.Error("expected step to have a handler")
	}

	handlerStep := program.Instructions[step.Handler].(*ir.StepInstruction)
	if handlerStep.DefinitionName != "retry" {
		t.Errorf("expected handler step 'retry', got %s", handlerStep.DefinitionName)
	}
}
