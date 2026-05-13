package plugin

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

type TestBindInput struct {
	HeddleFrame
	A *Float64
	B *Float64
}

type TestBindOutput struct {
	HeddleFrame
	A *Float64
	B *Float64
}

func TestBindWithRegistrationNewInputOutput(t *testing.T) {
	reg := &StepRegistration{
		InputType:              reflect.TypeFor[*TestBindInput](),
		OutputType:             reflect.TypeFor[*TestBindOutput](),
		inputHeddleFrameIndex:  0,
		outputHeddleFrameIndex: 0,
		inputFieldsIndex:       []int{1, 2},
		outputFieldsIndex:      []int{1, 2},
	}

	columns := make(map[string]arrow.Array)
	columns["A"] = NewFloat64([]float64{1.1, 2.2}).arrayFloat64
	columns["B"] = NewFloat64([]float64{1.1, 2.2}).arrayFloat64

	input, output := reg.NewInputOutput()
	assert.NotNil(t, input)
	assert.NotNil(t, output)

	err := bind(input, reg.inputFieldsIndex, columns)
	assert.NoError(t, err)

	assert.Equal(t, 1.1, input.Interface().(*TestBindInput).A.Value(0))
	assert.Equal(t, 2.2, input.Interface().(*TestBindInput).A.Value(1))
	assert.Equal(t, 1.1, input.Interface().(*TestBindInput).B.Value(0))
	assert.Equal(t, 2.2, input.Interface().(*TestBindInput).B.Value(1))

	// Now bind to output
	err = bind(output, reg.outputFieldsIndex, columns)
	assert.NoError(t, err)

	assert.Equal(t, 1.1, output.Interface().(*TestBindOutput).A.Value(0))
	assert.Equal(t, 2.2, output.Interface().(*TestBindOutput).A.Value(1))
	assert.Equal(t, 1.1, output.Interface().(*TestBindOutput).B.Value(0))
	assert.Equal(t, 2.2, output.Interface().(*TestBindOutput).B.Value(1))
}

type TestFrame struct {
	HeddleFrame
	A *Float64
	B *Float64
}

func TestBindAllColumns(t *testing.T) {

	columns := make(map[string]arrow.Array)
	columns["A"] = NewFloat64([]float64{1.1, 2.2}).arrayFloat64
	columns["B"] = NewFloat64([]float64{1.1, 2.2}).arrayFloat64

	frameType := reflect.TypeFor[*TestFrame]()
	frameValue := reflect.New(frameType.Elem())
	v := frameValue.Elem()
	v.Field(1).Set(reflect.New(v.Field(1).Type().Elem()))
	v.Field(2).Set(reflect.New(v.Field(2).Type().Elem()))

	err := bind(frameValue, []int{1, 2}, columns)
	assert.NoError(t, err)

	frame := frameValue.Interface().(*TestFrame)
	assert.NotNil(t, frame.A.arrayFloat64)
	assert.NotNil(t, frame.B.arrayFloat64)
	assert.Equal(t, 2, frame.A.Len())
	assert.Equal(t, 2, frame.B.Len())
}

func TestBindPartialColumns(t *testing.T) {
	columns := make(map[string]arrow.Array)
	columns["A"] = NewFloat64([]float64{1.1, 2.2}).arrayFloat64

	frameType := reflect.TypeFor[*TestFrame]()
	frameValue := reflect.New(frameType.Elem())
	v := frameValue.Elem()
	v.Field(1).Set(reflect.New(v.Field(1).Type().Elem()))
	v.Field(2).Set(reflect.New(v.Field(2).Type().Elem()))

	err := bind(frameValue, []int{1, 2}, columns)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "column \"B\" is required but missing")
}
