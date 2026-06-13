package std

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v18/arrow"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

// ExecutePrint implements std/io.print as an internal step.
func ExecutePrint(ctx context.Context, request plugin.ExecuteStepRequest) (plugin.ExecuteStepResponse, error) {
	columns := make(map[string]arrow.Array)
	for fieldName, path := range request.InputRef {
		arr, err := locality.ReadArrowArrayFromPath(path)
		if err != nil {
			logger.L().Error("Failed to read input from SHM", logger.Error(err), logger.String("path", path))
		} else {
			columns[fieldName] = arr
			defer arr.Release()
		}
	}

	var w io.Writer = os.Stdout
	if ctxWriter := plugin.GetOutputWriter(ctx); ctxWriter != nil {
		w = ctxWriter
	}

	fmt.Fprintf(w, "--- std/io.print ---\n")

	for name, arr := range columns {
		fmt.Fprintf(w, "\t%s: ", name)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				fmt.Fprintln(w, "<null>")
			} else {
				fmt.Fprintln(w, arr.ValueStr(i))
			}
		}
	}

	fmt.Fprintf(w, "--------------------\n")

	return plugin.ExecuteStepResponse{
		TaskID: request.TaskID,
		Status: plugin.StepResponseSuccess,
	}, nil
}

// ExecuteLoadCSV implements std/io.load_csv as an internal step.
func ExecuteLoadCSV(ctx context.Context, request plugin.ExecuteStepRequest) (plugin.ExecuteStepResponse, error) {
	// Parse config to get path and other csv options
	var config map[string]any
	if err := json.Unmarshal([]byte(request.ConfigJSON), &config); err != nil {
		logger.L().Error("execute load csv failed: invalid config", logger.Component("std/io"), logger.TraceID(request.WorkflowID), logger.TaskID(request.TaskID), logger.Error(err))
		return plugin.ExecuteStepResponse{TaskID: request.TaskID, Status: plugin.StepResponseError, ErrorMessage: "invalid config"}, nil
	}
	pathStr, ok := config["path"].(string)
	if !ok || pathStr == "" {
		logger.L().Error("execute load csv failed: path is required in config", logger.Component("std/io"), logger.TraceID(request.WorkflowID), logger.TaskID(request.TaskID))
		return plugin.ExecuteStepResponse{TaskID: request.TaskID, Status: plugin.StepResponseError, ErrorMessage: "path is required in config"}, nil
	}

	sender := models.GetControlSender(ctx)
	if sender == nil {
		logger.L().Error("execute load csv failed: control sender not found in context", logger.Component("std/io"), logger.TraceID(request.WorkflowID), logger.TaskID(request.TaskID))
		return plugin.ExecuteStepResponse{TaskID: request.TaskID, Status: plugin.StepResponseError, ErrorMessage: "control sender not found in context"}, nil
	}

	waiter := models.GetUploadWaiter(ctx)
	if waiter == nil {
		logger.L().Error("execute load csv failed: upload waiter not found in context", logger.Component("std/io"), logger.TraceID(request.WorkflowID), logger.TaskID(request.TaskID))
		return plugin.ExecuteStepResponse{TaskID: request.TaskID, Status: plugin.StepResponseError, ErrorMessage: "upload waiter not found in context"}, nil
	}

	// Parse columns config mapping: column_name -> data_type
	columnsMap := make(map[string]string)
	if cols, ok := config["columns"].(map[string]any); ok {
		for k, val := range cols {
			if strVal, ok := val.(string); ok {
				columnsMap[k] = strVal
			}
		}
	}

	// Forward all remaining options to the client
	optionsMap := make(map[string]any)
	for k, val := range config {
		if k != "path" && k != "columns" {
			optionsMap[k] = val
		}
	}

	fileReq := &models.FileRequest{
		WorkflowID: request.WorkflowID,
		TaskID:     request.TaskID,
		FilePath:   pathStr,
		Options:    optionsMap,
		Columns:    columnsMap,
	}

	err := sender(&models.ControlMessage{
		Type:        models.ActionRequestFile,
		FileRequest: fileReq,
	})
	if err != nil {
		logger.L().Error("execute load csv failed: failed to request file", logger.Component("std/io"), logger.TraceID(request.WorkflowID), logger.TaskID(request.TaskID), logger.Error(err))
		return plugin.ExecuteStepResponse{TaskID: request.TaskID, Status: plugin.StepResponseError, ErrorMessage: "failed to request file: " + err.Error()}, nil
	}

	logger.L().Info("execute load csv: waiting for file upload from client", logger.Component("std/io"), logger.TraceID(request.WorkflowID), logger.TaskID(request.TaskID), logger.String("path", pathStr))

	// Block until the file is received
	outputHandles, err := waiter(ctx, request.TaskID)
	if err != nil {
		logger.L().Error("execute load csv failed: error waiting for upload", logger.Component("std/io"), logger.TraceID(request.WorkflowID), logger.TaskID(request.TaskID), logger.Error(err))
		return plugin.ExecuteStepResponse{TaskID: request.TaskID, Status: plugin.StepResponseError, ErrorMessage: "error waiting for upload: " + err.Error()}, nil
	}

	logger.L().Info("execute load csv: file upload received", logger.Component("std/io"), logger.TraceID(request.WorkflowID), logger.TaskID(request.TaskID))

	return plugin.ExecuteStepResponse{
		TaskID:    request.TaskID,
		Status:    plugin.StepResponseSuccess,
		OutputRef: outputHandles,
	}, nil
}
