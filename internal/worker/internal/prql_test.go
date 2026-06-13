package internal

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

func TestExecutePRQL(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create column "val" (Float64)
	b := array.NewFloat64Builder(mem)
	b.AppendValues([]float64{1.2, 3.4, 5.6}, nil)
	arr := b.NewArray()
	defer arr.Release()

	path, err := locality.WriteArrowArrayToShm(arrow.Field{Name: "val", Type: arrow.PrimitiveTypes.Float64}, arr)
	require.NoError(t, err)
	defer os.Remove(path)

	// Create column "id" (Int32)
	b2 := array.NewInt32Builder(mem)
	b2.AppendValues([]int32{1, 2, 3}, nil)
	arr2 := b2.NewArray()
	defer arr2.Release()

	path2, err := locality.WriteArrowArrayToShm(arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32}, arr2)
	require.NoError(t, err)
	defer os.Remove(path2)

	// PRQL query: filter where id > 1
	configJSON, err := json.Marshal(map[string]any{
		"query": "from input | filter id > 1 | select {id, val}",
	})
	require.NoError(t, err)

	req := plugin.ExecuteStepRequest{
		WorkflowID: "wf-prql",
		TaskID:     "task-prql",
		StepName:   "prql",
		ConfigJSON: string(configJSON),
		InputRef: map[string]string{
			"val": path,
			"id":  path2,
		},
	}

	res, err := ExecutePRQL(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, plugin.StepResponseSuccess, res.Status)

	// Verify outputs
	require.Contains(t, res.OutputRef, "id")
	require.Contains(t, res.OutputRef, "val")

	idPath := res.OutputRef["id"]
	valPath := res.OutputRef["val"]

	defer os.Remove(idPath)
	defer os.Remove(valPath)

	idOutArr, err := locality.ReadArrowArrayFromPath(idPath)
	require.NoError(t, err)
	defer idOutArr.Release()

	valOutArr, err := locality.ReadArrowArrayFromPath(valPath)
	require.NoError(t, err)
	defer valOutArr.Release()

	// id > 1 means only ids 2 and 3 should be present
	assert.Equal(t, 2, idOutArr.Len())
	assert.Equal(t, arrow.PrimitiveTypes.Int32, idOutArr.DataType())
	
	idValues := idOutArr.(*array.Int32).Int32Values()
	assert.Equal(t, []int32{2, 3}, idValues)

	assert.Equal(t, 2, valOutArr.Len())
	assert.Equal(t, arrow.PrimitiveTypes.Float64, valOutArr.DataType())
	
	valValues := valOutArr.(*array.Float64).Float64Values()
	assert.Equal(t, []float64{3.4, 5.6}, valValues)
}

func TestExecutePRQL_Validation(t *testing.T) {
	tests := []struct {
		name       string
		configJSON string
		wantErr    string
	}{
		{
			name:       "Missing config JSON",
			configJSON: "",
			wantErr:    "prql: missing step config JSON",
		},
		{
			name:       "Malformed JSON",
			configJSON: "{invalid-json}",
			wantErr:    "prql: failed to parse config JSON",
		},
		{
			name:       "Empty query",
			configJSON: `{"query": ""}`,
			wantErr:    "prql: config must specify 'query'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := plugin.ExecuteStepRequest{
				WorkflowID: "wf-prql",
				TaskID:     "task-prql",
				StepName:   "prql",
				ConfigJSON: tt.configJSON,
				InputRef:   map[string]string{},
			}
			res, err := ExecutePRQL(context.Background(), task)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Empty(t, res.OutputRef)
		})
	}
}

func TestExecutePRQL_InnerJoin(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Table A columns
	b_id_a := array.NewInt32Builder(mem)
	b_id_a.AppendValues([]int32{1, 2, 3}, nil)
	arr_id_a := b_id_a.NewArray()
	defer arr_id_a.Release()
	path_id_a, err := locality.WriteArrowArrayToShm(arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32}, arr_id_a)
	require.NoError(t, err)
	defer os.Remove(path_id_a)

	b_val_a := array.NewFloat64Builder(mem)
	b_val_a.AppendValues([]float64{10.0, 20.0, 30.0}, nil)
	arr_val_a := b_val_a.NewArray()
	defer arr_val_a.Release()
	path_val_a, err := locality.WriteArrowArrayToShm(arrow.Field{Name: "val_a", Type: arrow.PrimitiveTypes.Float64}, arr_val_a)
	require.NoError(t, err)
	defer os.Remove(path_val_a)

	// Table B columns
	b_id_b := array.NewInt32Builder(mem)
	b_id_b.AppendValues([]int32{2, 3, 4}, nil)
	arr_id_b := b_id_b.NewArray()
	defer arr_id_b.Release()
	path_id_b, err := locality.WriteArrowArrayToShm(arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32}, arr_id_b)
	require.NoError(t, err)
	defer os.Remove(path_id_b)

	b_val_b := array.NewFloat64Builder(mem)
	b_val_b.AppendValues([]float64{200.0, 300.0, 400.0}, nil)
	arr_val_b := b_val_b.NewArray()
	defer arr_val_b.Release()
	path_val_b, err := locality.WriteArrowArrayToShm(arrow.Field{Name: "val_b", Type: arrow.PrimitiveTypes.Float64}, arr_val_b)
	require.NoError(t, err)
	defer os.Remove(path_val_b)

	// PRQL Query joining table_a and table_b
	configJSON, err := json.Marshal(map[string]any{
		"query": "from table_a | join table_b (==id) | select {table_a.id, table_a.val_a, table_b.val_b}",
	})
	require.NoError(t, err)

	req := plugin.ExecuteStepRequest{
		WorkflowID: "wf-prql-join",
		TaskID:     "task-prql-join",
		StepName:   "prql",
		ConfigJSON: string(configJSON),
		InputRef: map[string]string{
			"table_a_id":    path_id_a,
			"table_a_val_a": path_val_a,
			"table_b_id":    path_id_b,
			"table_b_val_b": path_val_b,
		},
	}

	res, err := ExecutePRQL(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, plugin.StepResponseSuccess, res.Status)

	// Verify outputs: Joined table should have ids 2 and 3
	require.Contains(t, res.OutputRef, "id")
	require.Contains(t, res.OutputRef, "val_a")
	require.Contains(t, res.OutputRef, "val_b")

	idPath := res.OutputRef["id"]
	valAPath := res.OutputRef["val_a"]
	valBPath := res.OutputRef["val_b"]

	defer os.Remove(idPath)
	defer os.Remove(valAPath)
	defer os.Remove(valBPath)

	idOutArr, err := locality.ReadArrowArrayFromPath(idPath)
	require.NoError(t, err)
	defer idOutArr.Release()

	valAOutArr, err := locality.ReadArrowArrayFromPath(valAPath)
	require.NoError(t, err)
	defer valAOutArr.Release()

	valBOutArr, err := locality.ReadArrowArrayFromPath(valBPath)
	require.NoError(t, err)
	defer valBOutArr.Release()

	assert.Equal(t, 2, idOutArr.Len())
	idValues := idOutArr.(*array.Int32).Int32Values()
	assert.Equal(t, []int32{2, 3}, idValues)

	assert.Equal(t, 2, valAOutArr.Len())
	valAValues := valAOutArr.(*array.Float64).Float64Values()
	assert.Equal(t, []float64{20.0, 30.0}, valAValues)

	assert.Equal(t, 2, valBOutArr.Len())
	valBValues := valBOutArr.(*array.Float64).Float64Values()
	assert.Equal(t, []float64{200.0, 300.0}, valBValues)
}

func TestExecutePRQL_WithPipedInput(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create column "val" (Float64)
	b := array.NewFloat64Builder(mem)
	b.AppendValues([]float64{1.2, 3.4, 5.6}, nil)
	arr := b.NewArray()
	defer arr.Release()

	path, err := locality.WriteArrowArrayToShm(arrow.Field{Name: "val", Type: arrow.PrimitiveTypes.Float64}, arr)
	require.NoError(t, err)
	defer os.Remove(path)

	// Create column "id" (Int32)
	b2 := array.NewInt32Builder(mem)
	b2.AppendValues([]int32{1, 2, 3}, nil)
	arr2 := b2.NewArray()
	defer arr2.Release()

	path2, err := locality.WriteArrowArrayToShm(arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32}, arr2)
	require.NoError(t, err)
	defer os.Remove(path2)

	// PRQL query: filter where id > 1
	configJSON, err := json.Marshal(map[string]any{
		"query":       "from input | filter id > 1 | select {id, val}",
		"piped_input": "assigment2",
	})
	require.NoError(t, err)

	req := plugin.ExecuteStepRequest{
		WorkflowID: "wf-prql-piped",
		TaskID:     "task-prql-piped",
		StepName:   "prql",
		ConfigJSON: string(configJSON),
		InputRef: map[string]string{
			"assigment2_val": path,
			"assigment2_id":  path2,
		},
	}

	res, err := ExecutePRQL(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, plugin.StepResponseSuccess, res.Status)

	// Verify outputs: output columns should not be prefixed
	require.Contains(t, res.OutputRef, "id")
	require.Contains(t, res.OutputRef, "val")

	idPath := res.OutputRef["id"]
	valPath := res.OutputRef["val"]

	defer os.Remove(idPath)
	defer os.Remove(valPath)

	idOutArr, err := locality.ReadArrowArrayFromPath(idPath)
	require.NoError(t, err)
	defer idOutArr.Release()

	valOutArr, err := locality.ReadArrowArrayFromPath(valPath)
	require.NoError(t, err)
	defer valOutArr.Release()

	assert.Equal(t, 2, idOutArr.Len())
	idValues := idOutArr.(*array.Int32).Int32Values()
	assert.Equal(t, []int32{2, 3}, idValues)

	assert.Equal(t, 2, valOutArr.Len())
	valValues := valOutArr.(*array.Float64).Float64Values()
	assert.Equal(t, []float64{3.4, 5.6}, valValues)
}

