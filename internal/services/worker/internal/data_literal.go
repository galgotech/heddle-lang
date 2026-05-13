package internal

import (
	"context"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

func ExecuteDataLiteral(ctx context.Context, task models.StepExecutionTask, registry *locality.DataLocalityRegistry) (models.TaskResult, error) {
	logger.L().Info("Executing data_literal step", zap.String("task_id", task.TaskID))

	data, ok := task.Step.Config["data"]
	if !ok {
		return models.TaskResult{}, fmt.Errorf("data_literal: missing 'data' in config")
	}

	listData, ok := data.([]map[string]any)
	if !ok {
		return models.TaskResult{}, fmt.Errorf("data_literal: 'data' must be a list of objects")
	}

	record, err := convertToArrowRecord(listData)
	if err != nil {
		return models.TaskResult{}, fmt.Errorf("data_literal: failed to convert to arrow: %w", err)
	}
	defer record.Release()

	// Store data in the locality registry for zero-copy access by plugins.
	// The task ID serves as the handle for subsequent steps.
	handle := task.TaskID

	paths := make(map[string]string)
	schema := record.Schema()
	for i := 0; i < int(record.NumCols()); i++ {
		field := schema.Field(i)
		arr := record.Column(i)
		path, err := locality.WriteArrowArrayToShm(field, arr)
		if err != nil {
			return models.TaskResult{}, fmt.Errorf("data_literal: failed to write column %s to SHM: %w", field.Name, err)
		}
		paths[field.Name] = path
		logger.L().Info("Allocated data_literal column to SHM", zap.String("handle", handle), zap.String("field", field.Name), zap.String("path", path))
	}

	// literal_data always is first step the tips is void -> data_type
	isOutputVoid := len(task.Step.OutputType) > 0 && task.Step.OutputType[0] == models.VoidType
	if isOutputVoid {
		logger.L().Warn("data_literal is first step and output is void, this is not expected", zap.String("handle", handle))
	}

	// Register the data in the locality registry
	if err := registry.Put(locality.NewMetadata(task.WorkflowID, handle, locality.Output, paths)); err != nil {
		return models.TaskResult{}, fmt.Errorf("data_literal: failed to register in locality registry: %w", err)
	}

	return models.TaskResult{
		TaskID: task.TaskID,
		Status: models.TaskStatusSuccess,
	}, nil
}

func convertToArrowRecord(data []map[string]any) (arrow.Record, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
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
