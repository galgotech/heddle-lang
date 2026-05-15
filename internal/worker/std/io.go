package std

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

// ExecutePrint implements std/io:print as an internal step.
func ExecutePrint(ctx context.Context, task models.StepExecutionTask, registry *locality.DataLocalityRegistry) (models.TaskResult, error) {
	// 1. Get input handle (the previous task's output)
	handle := task.PreviousTaskID
	if handle == "" {
		return models.TaskResult{}, fmt.Errorf("std/io:print: missing input handle (previous_task_id)")
	}

	// 2. Get metadata from locality registry
	meta, ok := registry.GetMetadata(task.WorkflowID, handle, locality.Output)
	if !ok {
		return models.TaskResult{}, fmt.Errorf("std/io:print: input handle %s not found in registry", handle)
	}

	// 3. Find the column to print. In Heddle, steps usually operate on specific fields.
	// For std/io:print, we'll look for a field named "print" or just print the first available column if not found.
	path, ok := meta.Paths["print"]
	if !ok {
		// Fallback: try "Print" (case sensitivity)
		path, ok = meta.Paths["Print"]
	}

	if !ok {
		// Fallback: use the first column if only one exists
		if len(meta.Paths) == 1 {
			for _, p := range meta.Paths {
				path = p
				break
			}
		} else {
			return models.TaskResult{}, fmt.Errorf("std/io:print: could not find 'print' column in input")
		}
	}

	// 4. Read the Arrow array from SHM
	arr, err := locality.ReadArrowArrayFromPath(path)
	if err != nil {
		return models.TaskResult{}, fmt.Errorf("std/io:print: failed to read arrow array: %w", err)
	}
	defer arr.Release()

	// 5. Print the values
	fmt.Printf("--- std/io:print ---\n")

	// We handle string arrays primarily for now
	if strArr, ok := arr.(*array.String); ok {
		for i := 0; i < strArr.Len(); i++ {
			if strArr.IsNull(i) {
				fmt.Println("<null>")
			} else {
				fmt.Println(strArr.Value(i))
			}
		}
	} else {
		// Generic printer for other types
		for i := 0; i < arr.Len(); i++ {
			fmt.Printf("%v\n", arr.ValueStr(i))
		}
	}

	fmt.Printf("--------------------\n")

	return models.TaskResult{
		TaskID: task.TaskID,
		Status: models.TaskStatusSuccess,
	}, nil
}
