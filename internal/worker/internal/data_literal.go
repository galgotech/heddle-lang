package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

func ExecuteDataLiteral(ctx context.Context, request plugin.ExecuteStepRequest) (plugin.ExecuteStepResponse, error) {
	logger.L().Info("Executing data_literal step", zap.String("task_id", request.TaskID))

	if request.ConfigJSON == "" {
		return plugin.ExecuteStepResponse{}, fmt.Errorf("data_literal: missing step config JSON")
	}

	var cfg map[string]any
	if err := json.Unmarshal([]byte(request.ConfigJSON), &cfg); err != nil {
		return plugin.ExecuteStepResponse{}, fmt.Errorf("data_literal: failed to parse config JSON: %w", err)
	}

	raw, ok := cfg["literalData"]
	if !ok || raw == nil {
		return plugin.ExecuteStepResponse{}, fmt.Errorf("data_literal: missing 'literalData' in step config")
	}

	slice, ok := raw.([]any)
	if !ok {
		return plugin.ExecuteStepResponse{}, fmt.Errorf("data_literal: 'literalData' must be a valid slice of maps, got %T", raw)
	}

	listData := make([]map[string]any, len(slice))
	for i, item := range slice {
		m, ok := item.(map[string]any)
		if !ok {
			return plugin.ExecuteStepResponse{}, fmt.Errorf("data_literal: element %d in 'literalData' is not a valid map, got %T", i, item)
		}
		listData[i] = m
	}

	record, err := convertToArrowRecord(listData)
	if err != nil {
		return plugin.ExecuteStepResponse{}, fmt.Errorf("data_literal: failed to convert to arrow: %w", err)
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
			return plugin.ExecuteStepResponse{}, fmt.Errorf("data_literal: failed to write column %s to SHM: %w", field.Name, err)
		}
		paths[field.Name] = path
		logger.L().Info("Allocated data_literal column to SHM", zap.String("handle", handle), zap.String("field", field.Name), zap.String("path", path))
	}

	return plugin.ExecuteStepResponse{
		TaskID:        request.TaskID,
		Status:        plugin.StepResponseSuccess,
		OutputHandles: paths,
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
