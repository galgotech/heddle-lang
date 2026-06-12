package orchestrator

import (
	"fmt"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

// ValidateEdge checks the schema compatibility of an edge in the DAG between fromID and toID.
func ValidateEdge(prog ir.Program, fromID, toID string, schemas map[string]schema.StepSchemas) error {
	if fromID == "" {
		return nil
	}

	fromStep, ok1 := prog.Instructions[fromID].(ir.StepInstruction)
	toStep, ok2 := prog.Instructions[toID].(ir.StepInstruction)
	if !ok1 || !ok2 {
		logger.L().Debug("edge validation skipped: instructions are not both steps",
			logger.Component("orchestrator-validation"),
			logger.String("from", fromID),
			logger.String("to", toID),
		)
		return nil
	}

	fromCap := fmt.Sprintf("%s.%s", fromStep.Call[0], fromStep.Call[1])
	toCap := fmt.Sprintf("%s.%s", toStep.Call[0], toStep.Call[1])

	logger.L().Debug("edge validation initiated: checking schema compatibility between nodes",
		logger.Component("orchestrator-validation"),
		logger.String("from_step", fromID),
		logger.String("to_step", toID),
		logger.Capability(fromCap),
		logger.Capability(toCap),
	)

	fromSchema, fromOk := schemas[fromCap]
	toSchema, toOk := schemas[toCap]

	if !fromOk || !toOk {
		logger.L().Warn("edge validation incomplete: missing schema definitions for validation",
			logger.Component("orchestrator-validation"),
			logger.String("from_step", fromID),
			logger.String("to_step", toID),
			logger.Capability(fromCap),
			logger.Capability(toCap),
			logger.Any("from_schema_exists", fromOk),
			logger.Any("to_schema_exists", toOk),
		)
		return nil
	}

	if err := schema.Compatible(fromSchema.Output, toSchema.Input); err != nil {
		logger.L().Error("edge validation failed: dag type error mismatch",
			logger.Component("orchestrator-validation"),
			logger.String("from_step", fromID),
			logger.String("to_step", toID),
			logger.Capability(fromCap),
			logger.Capability(toCap),
			logger.Error(err),
		)
		return fmt.Errorf("DAG Type Error: %s -> %s: %w", fromCap, toCap, err)
	}

	logger.L().Info("edge validation completed: schema is compatible",
		logger.Component("orchestrator-validation"),
		logger.String("from_step", fromID),
		logger.String("to_step", toID),
		logger.Capability(fromCap),
		logger.Capability(toCap),
	)
	return nil
}
