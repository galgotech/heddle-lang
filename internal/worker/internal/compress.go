package internal

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

func ExecuteCompress(ctx context.Context, task models.StepExecutionTask, registry *locality.DataLocalityRegistry) (models.TaskResult, error) {
	logger.L().Info("Executing compress step", zap.String("task_id", task.TaskID))

	// 1. Resolve input metadata to get SHM path and Dirty path
	meta, ok := registry.GetMetadata(task.WorkflowID, task.PreviousTaskID, locality.Output)
	if !ok {
		return models.TaskResult{}, fmt.Errorf("compress: input metadata not found for task %s", task.PreviousTaskID)
	}

	if len(meta.Paths) == 0 {
		return models.TaskResult{}, fmt.Errorf("compress: input paths are empty")
	}

	// Since compress needs the whole record, we have to construct it or do this per column.
	// For now, let's process column by column.
	resPaths := make(map[string]string)

	// 3. Read Dirty Bitmap. Assuming compress can use any of the dirty bitmaps, or they are identical.
	// In a column-wise setup, we might need a common dirty path or assume they are combined.
	// Let's just grab the first one we find.
	var dirtyPath string
	for _, dp := range meta.DirtyPaths {
		if dp != "" {
			dirtyPath = dp
			break
		}
	}

	if dirtyPath == "" {
		// No dirty bits, identity operation
		if err := registry.Put(locality.NewMetadata(task.WorkflowID, task.TaskID, locality.Output, meta.Paths)); err != nil {
			return models.TaskResult{}, err
		}
		return models.TaskResult{TaskID: task.TaskID, Status: models.TaskStatusSuccess}, nil
	}

	dirty, err := locality.ReadDirtyFromPath(dirtyPath)
	if err != nil {
		return models.TaskResult{}, fmt.Errorf("compress: failed to read dirty bits: %w", err)
	}

	mem := memory.NewGoAllocator()

	for fieldName, path := range meta.Paths {
		arr, err := locality.ReadArrowArrayFromPath(path)
		if err != nil {
			return models.TaskResult{}, fmt.Errorf("compress: failed to read input for %s: %w", fieldName, err)
		}

		validRows := make([]int, 0, arr.Len())
		for i := 0; i < arr.Len(); i++ {
			isDirty := (dirty[i/64] & (1 << (uint(i) % 64))) != 0
			if !isDirty {
				validRows = append(validRows, i)
			}
		}

		if len(validRows) == arr.Len() {
			resPaths[fieldName] = path
			arr.Release()
			continue
		}

		builder := array.NewBuilder(mem, arr.DataType())
		for _, rowIdx := range validRows {
			appendRecordValue(builder, arr, rowIdx)
		}
		newArr := builder.NewArray()
		builder.Release()
		arr.Release()

		field := arrow.Field{Name: fieldName, Type: newArr.DataType(), Nullable: true}
		path, err := locality.WriteArrowArrayToShm(field, newArr)
		if err != nil {
			newArr.Release()
			return models.TaskResult{}, fmt.Errorf("compress: failed to write result: %w", err)
		}
		resPaths[fieldName] = path
		newArr.Release()
	}

	if err := registry.Put(locality.NewMetadata(task.WorkflowID, task.TaskID, locality.Output, resPaths)); err != nil {
		return models.TaskResult{}, err
	}

	return models.TaskResult{
		TaskID: task.TaskID,
		Status: models.TaskStatusSuccess,
	}, nil
}

func appendRecordValue(b array.Builder, arr arrow.Array, rowIdx int) {
	if arr.IsNull(rowIdx) {
		b.AppendNull()
		return
	}

	switch builder := b.(type) {
	case *array.Int64Builder:
		builder.Append(arr.(*array.Int64).Value(rowIdx))
	case *array.Int32Builder:
		builder.Append(arr.(*array.Int32).Value(rowIdx))
	case *array.StringBuilder:
		builder.Append(arr.(*array.String).Value(rowIdx))
	case *array.BooleanBuilder:
		builder.Append(arr.(*array.Boolean).Value(rowIdx))
	case *array.Float64Builder:
		builder.Append(arr.(*array.Float64).Value(rowIdx))
	}
}
