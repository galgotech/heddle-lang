package std

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/compute"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

// ExecuteCast implements std/col.cast as an internal step.
func ExecuteCast(ctx context.Context, request plugin.ExecuteStepRequest) (plugin.ExecuteStepResponse, error) {
	compField := logger.Component("worker")
	traceField := logger.TraceID(request.WorkflowID)
	taskField := logger.TaskID(request.TaskID)

	logger.L().Info("step execution initiated: executing cast step", compField, traceField, taskField)

	if request.ConfigJSON == "" {
		err := fmt.Errorf("cast: missing step config JSON")
		logger.L().Error("step execution failed: missing step configuration json", compField, traceField, taskField, logger.Error(err))
		return plugin.ExecuteStepResponse{}, err
	}

	var cfg struct {
		Columns map[string]string `json:"columns"`
		To      string            `json:"to"`
	}
	if err := json.Unmarshal([]byte(request.ConfigJSON), &cfg); err != nil {
		wrappedErr := fmt.Errorf("cast: failed to parse config JSON: %w", err)
		logger.L().Error("step execution failed: failed to parse configuration json payload", compField, traceField, taskField, logger.Error(err))
		return plugin.ExecuteStepResponse{}, wrappedErr
	}

	if len(cfg.Columns) == 0 && cfg.To == "" {
		err := fmt.Errorf("cast: config must specify either 'columns' map or global 'to' type")
		logger.L().Error("step execution failed: missing columns or to field in step config", compField, traceField, taskField, logger.Error(err))
		return plugin.ExecuteStepResponse{}, err
	}

	outputRef := make(map[string]string)

	for colName, path := range request.InputRef {
		var targetTypeStr string
		if cfg.To != "" {
			targetTypeStr = cfg.To
		} else if t, ok := cfg.Columns[colName]; ok {
			targetTypeStr = t
		}

		if targetTypeStr == "" {
			// Passthrough column without casting
			outputRef[colName] = path
			continue
		}

		targetType, err := parseDataType(targetTypeStr)
		if err != nil {
			logger.L().Error("step execution failed: invalid target data type", compField, traceField, taskField, logger.String("column", colName), logger.String("type", targetTypeStr), logger.Error(err))
			return plugin.ExecuteStepResponse{}, fmt.Errorf("cast: invalid type for column %s: %w", colName, err)
		}

		arr, err := locality.ReadArrowArrayFromPath(path)
		if err != nil {
			wrappedErr := fmt.Errorf("cast: failed to read column %s from SHM: %w", colName, err)
			logger.L().Error("step execution failed: failed to read column array from shared memory", compField, traceField, taskField, logger.String("column", colName), logger.Error(err))
			return plugin.ExecuteStepResponse{}, wrappedErr
		}

		// Perform cast
		logger.L().Debug("type casting initiated: casting column data type", compField, traceField, taskField, logger.String("column", colName), logger.String("target_type", targetType.Name()))
		castedArr, err := compute.CastArray(ctx, arr, &compute.CastOptions{ToType: targetType})
		arr.Release() // Release the input array since we are done with it
		if err != nil {
			wrappedErr := fmt.Errorf("cast: failed to cast column %s to %s: %w", colName, targetTypeStr, err)
			logger.L().Error("type casting failed: error casting column via arrow compute kernel", compField, traceField, taskField, logger.String("column", colName), logger.Error(err))
			return plugin.ExecuteStepResponse{}, wrappedErr
		}

		// Write casted array back to SHM preserving its name
		newPath, err := locality.WriteArrowArrayToShm(arrow.Field{Name: colName, Type: targetType, Nullable: true}, castedArr)
		castedArr.Release() // Release the casted array after writing to SHM
		if err != nil {
			wrappedErr := fmt.Errorf("cast: failed to write casted column %s to SHM: %w", colName, err)
			logger.L().Error("shm allocation failed: failed to write casted column array to shared memory segment", compField, traceField, taskField, logger.String("column", colName), logger.Error(err))
			return plugin.ExecuteStepResponse{}, wrappedErr
		}

		outputRef[colName] = newPath
		logger.L().Info("shm allocation completed: allocated casted column to shared memory", compField, traceField, taskField, logger.String("column", colName), logger.String("path", newPath))
	}

	logger.L().Info("step execution completed: successfully executed cast step", compField, traceField, taskField)
	return plugin.ExecuteStepResponse{
		TaskID:    request.TaskID,
		Status:    plugin.StepResponseSuccess,
		OutputRef: outputRef,
	}, nil
}

// parseDataType maps a string representation of a type to an arrow.DataType.
func parseDataType(t string) (arrow.DataType, error) {
	switch strings.ToLower(t) {
	case "int8":
		return arrow.PrimitiveTypes.Int8, nil
	case "int16":
		return arrow.PrimitiveTypes.Int16, nil
	case "int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "uint8":
		return arrow.PrimitiveTypes.Uint8, nil
	case "uint16":
		return arrow.PrimitiveTypes.Uint16, nil
	case "uint32":
		return arrow.PrimitiveTypes.Uint32, nil
	case "uint64":
		return arrow.PrimitiveTypes.Uint64, nil
	case "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "string", "utf8", "text":
		return arrow.BinaryTypes.String, nil
	case "bool", "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	default:
		return nil, fmt.Errorf("unsupported target data type: %s", t)
	}
}
