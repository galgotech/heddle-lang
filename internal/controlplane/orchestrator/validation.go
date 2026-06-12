package orchestrator

import (
	"fmt"
	"strings"

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

	// 1. Resolve output schema of fromStep
	var fromSchemaOutput []schema.ColumnSchema
	if fromSchema, fromOk := schemas[fromCap]; fromOk {
		fromSchemaOutput = fromSchema.Output
	}

	// Override dynamically if std step
	resolvedOut, err := resolveStepOutputSchema(prog, fromID, schemas)
	if err == nil && resolvedOut != nil {
		fromSchemaOutput = resolvedOut
	}

	// 2. Validate against toStep
	if toCap == "std/io.print" {
		for _, col := range fromSchemaOutput {
			if !isPrintableType(col.ArrowType) {
				return fmt.Errorf("DAG Type Error: print: column '%s' has unprintable type '%s'", col.Name, col.ArrowType)
			}
		}
		return nil
	}

	if toCap == "std/col.cast" {
		globalTo, _ := toStep.Config["to"].(string)
		columnsMap := make(map[string]string)
		if colsVal, ok := toStep.Config["columns"]; ok {
			if m, ok := colsVal.(map[string]any); ok {
				for k, v := range m {
					if s, ok := v.(string); ok {
						columnsMap[k] = s
					}
				}
			} else if m, ok := colsVal.(map[string]string); ok {
				for k, v := range m {
					columnsMap[k] = v
				}
			}
		}

		if len(columnsMap) == 0 && globalTo == "" {
			return fmt.Errorf("DAG Type Error: cast: config must specify either 'columns' map or global 'to' type")
		}

		for _, col := range fromSchemaOutput {
			var targetTypeStr string
			if globalTo != "" {
				targetTypeStr = globalTo
			} else if t, ok := columnsMap[col.Name]; ok {
				targetTypeStr = t
			}

			if targetTypeStr != "" {
				norm, err := normalizeArrowType(targetTypeStr)
				if err != nil {
					return fmt.Errorf("DAG Type Error: cast: invalid target type '%s' for column '%s': %w", targetTypeStr, col.Name, err)
				}
				if !isCastAllowed(col.ArrowType, norm) {
					return fmt.Errorf("DAG Type Error: cast: conversion from '%s' to '%s' is not supported for column '%s'", col.ArrowType, norm, col.Name)
				}
			}
		}
		return nil
	}

	var toSchemaInput []schema.ColumnSchema
	if toSchema, toOk := schemas[toCap]; toOk {
		toSchemaInput = toSchema.Input
	} else {
		// Missing schema info for target step, skip strict edge validation
		logger.L().Warn("edge validation incomplete: missing schema definitions for validation",
			logger.Component("orchestrator-validation"),
			logger.String("from_step", fromID),
			logger.String("to_step", toID),
			logger.Capability(fromCap),
			logger.Capability(toCap),
		)
		return nil
	}

	if err := schema.Compatible(fromSchemaOutput, toSchemaInput); err != nil {
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

func resolveStepOutputSchema(prog ir.Program, id string, schemas map[string]schema.StepSchemas) ([]schema.ColumnSchema, error) {
	if id == "" {
		return nil, nil
	}
	inst, ok := prog.Instructions[id]
	if !ok {
		return nil, nil
	}
	step, ok := inst.(ir.StepInstruction)
	if !ok {
		return nil, nil
	}

	cap := fmt.Sprintf("%s.%s", step.Call[0], step.Call[1])

	if cap == "std/col.cast" {
		var parentOut []schema.ColumnSchema
		if len(step.Parents) > 0 {
			var err error
			parentOut, err = resolveStepOutputSchema(prog, step.Parents[0], schemas)
			if err != nil {
				return nil, err
			}
		}

		globalTo, _ := step.Config["to"].(string)
		columnsMap := make(map[string]string)
		if colsVal, ok := step.Config["columns"]; ok {
			if m, ok := colsVal.(map[string]any); ok {
				for k, v := range m {
					if s, ok := v.(string); ok {
						columnsMap[k] = s
					}
				}
			} else if m, ok := colsVal.(map[string]string); ok {
				for k, v := range m {
					columnsMap[k] = v
				}
			}
		}

		out := make([]schema.ColumnSchema, len(parentOut))
		for i, col := range parentOut {
			out[i] = col
			var targetTypeStr string
			if globalTo != "" {
				targetTypeStr = globalTo
			} else if t, ok := columnsMap[col.Name]; ok {
				targetTypeStr = t
			}

			if targetTypeStr != "" {
				norm, err := normalizeArrowType(targetTypeStr)
				if err != nil {
					return nil, fmt.Errorf("cast: invalid target type '%s' for column '%s': %w", targetTypeStr, col.Name, err)
				}
				out[i].ArrowType = norm
			}
		}
		return out, nil
	}

	if cap == "std/io.print" {
		return nil, nil
	}

	if s, ok := schemas[cap]; ok {
		return s.Output, nil
	}

	return nil, nil
}

func isPrintableType(t string) bool {
	switch strings.ToLower(t) {
	case "int8", "int16", "int32", "int64",
		"uint8", "uint16", "uint32", "uint64",
		"float32", "float64",
		"string", "utf8", "text",
		"bool", "boolean":
		return true
	default:
		return false
	}
}

func normalizeArrowType(t string) (string, error) {
	switch strings.ToLower(t) {
	case "int8":
		return "int8", nil
	case "int16":
		return "int16", nil
	case "int32":
		return "int32", nil
	case "int64":
		return "int64", nil
	case "uint8":
		return "uint8", nil
	case "uint16":
		return "uint16", nil
	case "uint32":
		return "uint32", nil
	case "uint64":
		return "uint64", nil
	case "float32":
		return "float32", nil
	case "float64":
		return "float64", nil
	case "string", "utf8", "text":
		return "utf8", nil
	case "bool", "boolean":
		return "bool", nil
	default:
		return "", fmt.Errorf("unsupported type: %s", t)
	}
}

func isCastAllowed(from, to string) bool {
	_, err1 := normalizeArrowType(from)
	_, err2 := normalizeArrowType(to)
	return err1 == nil && err2 == nil
}
