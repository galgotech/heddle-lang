package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

func ExecuteDataLiteral(ctx context.Context, request plugin.ExecuteStepRequest) (plugin.ExecuteStepResponse, error) {
	compField := logger.Component("worker")
	traceField := logger.TraceID(request.WorkflowID)
	taskField := logger.TaskID(request.TaskID)

	logger.L().Info("step execution initiated: executing data_literal step", compField, traceField, taskField)

	if request.ConfigJSON == "" {
		err := fmt.Errorf("data_literal: missing step config JSON")
		logger.L().Error("step execution failed: missing step configuration json", compField, traceField, taskField, logger.Error(err))
		return plugin.ExecuteStepResponse{}, err
	}

	var cfg map[string]any
	if err := json.Unmarshal([]byte(request.ConfigJSON), &cfg); err != nil {
		wrappedErr := fmt.Errorf("data_literal: failed to parse config JSON: %w", err)
		logger.L().Error("step execution failed: failed to parse configuration json payload", compField, traceField, taskField, logger.Error(err))
		return plugin.ExecuteStepResponse{}, wrappedErr
	}

	raw, ok := cfg["literalData"]
	if !ok || raw == nil {
		err := fmt.Errorf("data_literal: missing 'literalData' in step config")
		logger.L().Error("step execution failed: missing literalData field in step config", compField, traceField, taskField, logger.Error(err))
		return plugin.ExecuteStepResponse{}, err
	}

	slice, ok := raw.([]any)
	if !ok {
		err := fmt.Errorf("data_literal: 'literalData' must be a valid slice of maps, got %T", raw)
		logger.L().Error("step execution failed: invalid literalData type in step config", compField, traceField, taskField, logger.Error(err))
		return plugin.ExecuteStepResponse{}, err
	}

	listData := make([]map[string]any, len(slice))
	for i, item := range slice {
		m, ok := item.(map[string]any)
		if !ok {
			err := fmt.Errorf("data_literal: element %d in 'literalData' is not a valid map, got %T", i, item)
			logger.L().Error("step execution failed: invalid map element in literalData", compField, traceField, taskField, logger.Int("index", i), logger.Error(err))
			return plugin.ExecuteStepResponse{}, err
		}
		listData[i] = m
	}

	if len(listData) == 0 {
		logger.L().Warn("step execution check: executing data_literal step with empty literalData array", compField, traceField, taskField)
	}

	logger.L().Debug("data conversion initiated: converting list data to arrow record", compField, traceField, taskField, logger.Int("rows", len(listData)))
	record, err := convertToArrowRecord(listData)
	if err != nil {
		wrappedErr := fmt.Errorf("data_literal: failed to convert to arrow: %w", err)
		logger.L().Error("data conversion failed: error converting map elements to arrow record structure", compField, traceField, taskField, logger.Error(err))
		return plugin.ExecuteStepResponse{}, wrappedErr
	}
	defer record.Release()

	// Store data in the locality registry for zero-copy access by plugins.
	// The task ID serves as the handle for subsequent steps.
	handle := request.TaskID

	paths := make(map[string]string)
	schema := record.Schema()
	for i := 0; i < int(record.NumCols()); i++ {
		field := schema.Field(i)
		arr := record.Column(i)
		path, err := locality.WriteArrowArrayToShm(field, arr)
		if err != nil {
			wrappedErr := fmt.Errorf("data_literal: failed to write column %s to SHM: %w", field.Name, err)
			logger.L().Error("shm allocation failed: failed to write column array to shared memory segment", compField, traceField, taskField, logger.String("field", field.Name), logger.Error(err))
			return plugin.ExecuteStepResponse{}, wrappedErr
		}
		paths[field.Name] = path
		logger.L().Info("shm allocation completed: allocated data_literal column to shared memory", compField, traceField, taskField, logger.String("handle", handle), logger.String("field", field.Name), logger.String("path", path))
	}

	logger.L().Info("step execution completed: successfully executed data_literal step", compField, traceField, taskField)
	return plugin.ExecuteStepResponse{
		TaskID:    request.TaskID,
		Status:    plugin.StepResponseSuccess,
		OutputRef: paths,
	}, nil
}

func convertToArrowRecord(data []map[string]any) (arrow.Record, error) {
	if data == nil {
		return nil, fmt.Errorf("data is nil")
	}

	if len(data) == 0 {
		schema := arrow.NewSchema(nil, nil)
		return array.NewRecord(schema, nil, 0), nil
	}

	first := data[0]

	// 1. Infer schema from first element
	keys := make([]string, 0, len(first))
	for k := range first {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fields := make([]arrow.Field, 0, len(keys))
	for _, k := range keys {
		v := first[k]
		var dt arrow.DataType
		switch v.(type) {
		case float64:
			dt = arrow.PrimitiveTypes.Float64
		case string:
			dt = arrow.BinaryTypes.String
		case bool:
			dt = arrow.FixedWidthTypes.Boolean
		default:
			dt = arrow.BinaryTypes.String // Fallback
		}
		fields = append(fields, arrow.Field{Name: k, Type: dt})
	}
	schema := arrow.NewSchema(fields, nil)

	// 2. Build columns
	mem := memory.NewGoAllocator()
	builders := make([]array.Builder, len(fields))
	for i, f := range fields {
		builders[i] = array.NewBuilder(mem, f.Type)
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	for _, item := range data {
		for i, f := range fields {
			val := item[f.Name]
			if val == nil {
				builders[i].AppendNull()
				continue
			}

			switch b := builders[i].(type) {
			case *array.Float64Builder:
				if fv, ok := val.(float64); ok {
					b.Append(fv)
				} else {
					b.AppendNull()
				}
			case *array.StringBuilder:
				b.Append(fmt.Sprint(val))
			case *array.BooleanBuilder:
				if bv, ok := val.(bool); ok {
					b.Append(bv)
				} else {
					b.AppendNull()
				}
			default:
				builders[i].AppendNull()
			}
		}
	}

	cols := make([]arrow.Array, len(fields))
	for i := range fields {
		cols[i] = builders[i].NewArray()
	}
	// Note: Caller is responsible for releasing the record, which releases these columns.
	return array.NewRecord(schema, cols, int64(len(data))), nil
}
