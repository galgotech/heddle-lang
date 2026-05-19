package std

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

// ExecutePrint implements std/io.print as an internal step.
func ExecutePrint(ctx context.Context, request plugin.ExecuteStepRequest) (plugin.ExecuteStepResponse, error) {
	columns := make(map[string]arrow.Array)
	for fieldName, path := range request.InputHandles {
		arr, err := locality.ReadArrowArrayFromPath(path)
		if err != nil {
			logger.L().Error("Failed to read input from SHM", zap.Error(err), zap.String("path", path))
		} else {
			columns[fieldName] = arr
			defer arr.Release()
		}
	}

	fmt.Printf("--- std/io.print ---\n")

	for name, arr := range columns {
		fmt.Printf("\t%s: ", name)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				fmt.Println("<null>")
			} else {
				fmt.Println(arr.ValueStr(i))
			}
		}
	}

	fmt.Printf("--------------------\n")

	return plugin.ExecuteStepResponse{
		TaskID: request.TaskID,
		Status: plugin.StepResponseSuccess,
	}, nil
}
