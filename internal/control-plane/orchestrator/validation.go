package orchestrator

import (
	"fmt"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

// ValidateEdge checks the schema compatibility of an edge in the DAG between fromID and toID.
func ValidateEdge(prog *ir.Program, fromID, toID string, schemas map[string]schema.StepSchemas) error {
	if fromID == "" {
		return nil
	}

	fromStep, ok1 := prog.Instructions[fromID].(ir.StepInstruction)
	toStep, ok2 := prog.Instructions[toID].(ir.StepInstruction)
	if !ok1 || !ok2 {
		return nil
	}

	fromCap := fmt.Sprintf("%s.%s", fromStep.Call[0], fromStep.Call[1])
	toCap := fmt.Sprintf("%s.%s", toStep.Call[0], toStep.Call[1])

	fromSchema, fromOk := schemas[fromCap]
	toSchema, toOk := schemas[toCap]

	if !fromOk || !toOk {
		return nil
	}

	if err := schema.Compatible(fromSchema.Output, toSchema.Input); err != nil {
		return fmt.Errorf("DAG Type Error: %s -> %s: %w", fromCap, toCap, err)
	}

	return nil
}
