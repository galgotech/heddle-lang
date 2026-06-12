package std

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

func TestExecuteCast_ToGlobalString(t *testing.T) {
	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem)
	b.AppendValues([]float64{1.2, 3.4, 5.6}, nil)
	arr := b.NewArray()
	defer arr.Release()

	path, err := locality.WriteArrowArrayToShm(arrow.Field{Name: "val", Type: arrow.PrimitiveTypes.Float64}, arr)
	require.NoError(t, err)
	defer os.Remove(path)

	configJSON, err := json.Marshal(map[string]any{
		"to": "string",
	})
	require.NoError(t, err)

	req := plugin.ExecuteStepRequest{
		WorkflowID: "wf-cast",
		TaskID:     "task-cast",
		StepName:   "cast",
		ConfigJSON: string(configJSON),
		InputRef: map[string]string{
			"val": path,
		},
	}

	res, err := ExecuteCast(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, plugin.StepResponseSuccess, res.Status)
	require.Contains(t, res.OutputRef, "val")

	castedPath := res.OutputRef["val"]
	require.NotEmpty(t, castedPath)
	defer os.Remove(castedPath)

	// Read back and verify the type is String and the values are correct
	castedArr, err := locality.ReadArrowArrayFromPath(castedPath)
	require.NoError(t, err)
	defer castedArr.Release()

	assert.Equal(t, arrow.BinaryTypes.String, castedArr.DataType())
	assert.Equal(t, 3, castedArr.Len())
	assert.Equal(t, "1.2", castedArr.ValueStr(0))
	assert.Equal(t, "3.4", castedArr.ValueStr(1))
	assert.Equal(t, "5.6", castedArr.ValueStr(2))
}

func TestExecuteCast_ColumnsSpecific(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create two columns: "id" (Float64) and "keep" (Float64)
	b1 := array.NewFloat64Builder(mem)
	b1.AppendValues([]float64{10, 20}, nil)
	arr1 := b1.NewArray()
	defer arr1.Release()

	b2 := array.NewFloat64Builder(mem)
	b2.AppendValues([]float64{1.1, 2.2}, nil)
	arr2 := b2.NewArray()
	defer arr2.Release()

	path1, err := locality.WriteArrowArrayToShm(arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Float64}, arr1)
	require.NoError(t, err)
	defer os.Remove(path1)

	path2, err := locality.WriteArrowArrayToShm(arrow.Field{Name: "keep", Type: arrow.PrimitiveTypes.Float64}, arr2)
	require.NoError(t, err)
	defer os.Remove(path2)

	// We only cast "id" to "int32", and keep "keep" as-is (passthrough)
	configJSON, err := json.Marshal(map[string]any{
		"columns": map[string]string{
			"id": "int32",
		},
	})
	require.NoError(t, err)

	req := plugin.ExecuteStepRequest{
		WorkflowID: "wf-cast",
		TaskID:     "task-cast-2",
		StepName:   "cast",
		ConfigJSON: string(configJSON),
		InputRef: map[string]string{
			"id":   path1,
			"keep": path2,
		},
	}

	res, err := ExecuteCast(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, plugin.StepResponseSuccess, res.Status)

	// "id" must be casted to int32
	castedIDPath := res.OutputRef["id"]
	require.NotEmpty(t, castedIDPath)
	defer os.Remove(castedIDPath)

	arrID, err := locality.ReadArrowArrayFromPath(castedIDPath)
	require.NoError(t, err)
	defer arrID.Release()
	assert.Equal(t, arrow.PrimitiveTypes.Int32, arrID.DataType())

	// "keep" must be passed through unchanged (same SHM path)
	assert.Equal(t, path2, res.OutputRef["keep"])
}

func TestExecuteCast_Validation(t *testing.T) {
	tests := []struct {
		name       string
		configJSON string
		wantErr    string
	}{
		{
			name:       "Missing config JSON",
			configJSON: "",
			wantErr:    "cast: missing step config JSON",
		},
		{
			name:       "Malformed JSON",
			configJSON: "{invalid-json}",
			wantErr:    "cast: failed to parse config JSON",
		},
		{
			name:       "Empty config keys",
			configJSON: `{"otherKey": 123}`,
			wantErr:    "cast: config must specify either 'columns' map or global 'to' type",
		},
		{
			name:       "Invalid data type",
			configJSON: `{"to": "invalid-type"}`,
			wantErr:    "cast: invalid type for column val",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := locality.WriteArrowArrayOnlyToShm(array.NewNull(1))
			require.NoError(t, err)
			defer os.Remove(path)

			task := plugin.ExecuteStepRequest{
				WorkflowID: "wf-val",
				TaskID:     "task-val",
				StepName:   "cast",
				ConfigJSON: tt.configJSON,
				InputRef: map[string]string{
					"val": path,
				},
			}
			res, err := ExecuteCast(context.Background(), task)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Empty(t, res)
		})
	}
}
