package internal

/*
#cgo LDFLAGS: -L${SRCDIR}/../../datafusion-ffi/target/release -ldatafusion_ffi -ldl -lm -lpthread
#include <stdint.h>
#include <stdlib.h>

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};

struct FFIColumn {
    const char* name;
    void* schema;
    void* array;
};

struct FFIColumnList {
    struct FFIColumn* columns;
    size_t count;
    char* error;
};

extern struct FFIColumnList execute_prql_query(const char* query, const struct FFIColumn* input_columns, size_t input_count);
extern void free_ffi_column_list(struct FFIColumnList list);
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unsafe"

	"github.com/apache/arrow/go/v18/arrow/cdata"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

func ExecutePRQL(ctx context.Context, request plugin.ExecuteStepRequest) (plugin.ExecuteStepResponse, error) {
	compField := logger.Component("worker")
	traceField := logger.TraceID(request.WorkflowID)
	taskField := logger.TaskID(request.TaskID)

	logger.L().Info("step execution initiated: executing prql step", compField, traceField, taskField)

	if request.ConfigJSON == "" {
		err := fmt.Errorf("prql: missing step config JSON")
		return plugin.ExecuteStepResponse{}, err
	}

	var cfg struct {
		Query      string `json:"query"`
		PipedInput string `json:"piped_input"`
	}
	if err := json.Unmarshal([]byte(request.ConfigJSON), &cfg); err != nil {
		return plugin.ExecuteStepResponse{}, fmt.Errorf("prql: failed to parse config JSON: %w", err)
	}

	if cfg.Query == "" {
		return plugin.ExecuteStepResponse{}, fmt.Errorf("prql: config must specify 'query'")
	}

	var cColumns []C.struct_FFIColumn
	
	var arraysToRelease []func()
	defer func() {
		for _, release := range arraysToRelease {
			release()
		}
	}()

	for colName, path := range request.InputRef {
		arr, err := locality.ReadArrowArrayFromPath(path)
		if err != nil {
			return plugin.ExecuteStepResponse{}, fmt.Errorf("prql: failed to read column %s from SHM: %w", colName, err)
		}
		
		cSchema := (*C.struct_ArrowSchema)(C.calloc(1, C.sizeof_struct_ArrowSchema))
		cArray := (*C.struct_ArrowArray)(C.calloc(1, C.sizeof_struct_ArrowArray))
		
		cdata.ExportArrowArray(arr, (*cdata.CArrowArray)(unsafe.Pointer(cArray)), (*cdata.CArrowSchema)(unsafe.Pointer(cSchema)))
		arraysToRelease = append(arraysToRelease, arr.Release)
		
		// Also free the malloc'd structs after execution
		defer C.free(unsafe.Pointer(cSchema))
		defer C.free(unsafe.Pointer(cArray))

		// If this column belongs to the piped input, strip its assignment name prefix.
		actualColName := colName
		if cfg.PipedInput != "" {
			prefix := cfg.PipedInput + "_"
			if strings.HasPrefix(colName, prefix) {
				actualColName = strings.TrimPrefix(colName, prefix)
			}
		}

		cColName := C.CString(actualColName)
		defer C.free(unsafe.Pointer(cColName))
		
		cColumns = append(cColumns, C.struct_FFIColumn{
			name:   cColName,
			schema: unsafe.Pointer(cSchema),
			array:  unsafe.Pointer(cArray),
		})
	}

	cQuery := C.CString(cfg.Query)
	defer C.free(unsafe.Pointer(cQuery))

	var cColumnsPtr *C.struct_FFIColumn
	if len(cColumns) > 0 {
		cColumnsPtr = &cColumns[0]
	}

	result := C.execute_prql_query(cQuery, cColumnsPtr, C.size_t(len(cColumns)))
	defer C.free_ffi_column_list(result)

	if result.error != nil {
		errMsg := C.GoString(result.error)
		return plugin.ExecuteStepResponse{}, fmt.Errorf("prql execution failed: %s", errMsg)
	}

	outputRef := make(map[string]string)
	
	outCount := int(result.count)
	if outCount > 0 {
		outSlice := unsafe.Slice(result.columns, outCount)
		for _, outCol := range outSlice {
			colName := C.GoString(outCol.name)
			
			cSchema := (*C.struct_ArrowSchema)(outCol.schema)
			cArray := (*C.struct_ArrowArray)(outCol.array)
			
			importedField, importedArr, err := cdata.ImportCArray((*cdata.CArrowArray)(unsafe.Pointer(cArray)), (*cdata.CArrowSchema)(unsafe.Pointer(cSchema)))
			if err != nil {
				return plugin.ExecuteStepResponse{}, fmt.Errorf("prql: failed to import result column %s: %w", colName, err)
			}
			defer importedArr.Release()
			
			importedField.Name = colName
			
			newPath, err := locality.WriteArrowArrayToShm(importedField, importedArr)
			if err != nil {
				return plugin.ExecuteStepResponse{}, fmt.Errorf("prql: failed to write result column %s to SHM: %w", colName, err)
			}
			
			outputRef[colName] = newPath
		}
	}

	logger.L().Info("step execution completed: successfully executed prql step", compField, traceField, taskField)
	return plugin.ExecuteStepResponse{
		TaskID:    request.TaskID,
		Status:    plugin.StepResponseSuccess,
		OutputRef: outputRef,
	}, nil
}
