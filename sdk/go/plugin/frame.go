package plugin

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/compute"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/arrow/scalar"
)

type arrayInt8 = array.Int8
type arrayInt16 = array.Int16
type arrayInt32 = array.Int32
type arrayInt64 = array.Int64
type arrayUint8 = array.Uint8
type arrayUint16 = array.Uint16
type arrayUint32 = array.Uint32
type arrayUint64 = array.Uint64
type arrayFloat32 = array.Float32
type arrayFloat64 = array.Float64
type arrayBool = array.Boolean
type arrayString = array.String

var pool = memory.NewGoAllocator()

type metaField struct {
	dirt []uint64
}

func (f *metaField) Delete(rowIndex int) {
	idx := rowIndex / 64
	for len(f.dirt) <= idx {
		f.dirt = append(f.dirt, 0)
	}
	f.dirt[idx] |= (1 << (uint(rowIndex) % 64))
}

type Empty struct {
	metaField
	*arrayInt8
}

func NewEmpty() *Empty {
	b := array.NewInt8Builder(pool)
	defer b.Release()
	return &Empty{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayInt8: b.NewArray().(*arrayInt8),
	}
}

type Int8 struct {
	metaField
	*arrayInt8
}

func NewInt8(data []int8) *Int8 {
	b := array.NewInt8Builder(pool)
	defer b.Release()
	b.AppendValues(data, nil)
	return &Int8{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayInt8: b.NewArray().(*arrayInt8),
	}
}

type Int16 struct {
	metaField
	*arrayInt16
}

func NewInt16(data []int16) *Int16 {
	b := array.NewInt16Builder(pool)
	defer b.Release()
	b.AppendValues(data, nil)
	return &Int16{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayInt16: b.NewArray().(*arrayInt16),
	}
}

type Int32 struct {
	metaField
	*arrayInt32
}

func NewInt32(data []int32) *Int32 {
	b := array.NewInt32Builder(pool)
	defer b.Release()
	b.AppendValues(data, nil)
	return &Int32{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayInt32: b.NewArray().(*arrayInt32),
	}
}

type Int64 struct {
	metaField
	*arrayInt64
}

func NewInt64(data []int64) *Int64 {
	b := array.NewInt64Builder(pool)
	defer b.Release()
	b.AppendValues(data, nil)
	return &Int64{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayInt64: b.NewArray().(*arrayInt64),
	}
}

type Uint8 struct {
	metaField
	*arrayUint8
}

func NewUint8(data []uint8) *Uint8 {
	b := array.NewUint8Builder(pool)
	defer b.Release()
	b.AppendValues(data, nil)
	return &Uint8{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayUint8: b.NewArray().(*arrayUint8),
	}
}

type Uint16 struct {
	metaField
	*arrayUint16
}

func NewUint16(data []uint16) *Uint16 {
	b := array.NewUint16Builder(pool)
	defer b.Release()
	b.AppendValues(data, nil)
	return &Uint16{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayUint16: b.NewArray().(*arrayUint16),
	}
}

type Uint32 struct {
	metaField
	*arrayUint32
}

func NewUint32(data []uint32) *Uint32 {
	b := array.NewUint32Builder(pool)
	defer b.Release()
	b.AppendValues(data, nil)
	return &Uint32{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayUint32: b.NewArray().(*arrayUint32),
	}
}

type Uint64 struct {
	metaField
	*arrayUint64
}

func NewUint64(data []uint64) *Uint64 {
	b := array.NewUint64Builder(pool)
	defer b.Release()
	b.AppendValues(data, nil)
	return &Uint64{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayUint64: b.NewArray().(*arrayUint64),
	}
}

type Float32 struct {
	metaField
	*arrayFloat32
}

func NewFloat32(data []float32) *Float32 {
	b := array.NewFloat32Builder(pool)
	defer b.Release()
	b.AppendValues(data, nil)
	return &Float32{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayFloat32: b.NewArray().(*arrayFloat32),
	}
}

type Float64 struct {
	metaField
	*arrayFloat64
}

func NewFloat64(data []float64) *Float64 {
	b := array.NewFloat64Builder(pool)
	defer b.Release()
	b.AppendValues(data, nil)
	return &Float64{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayFloat64: b.NewArray().(*arrayFloat64),
	}
}

type Bool struct {
	metaField
	*arrayBool
}

func NewBool(data []bool) *Bool {
	b := array.NewBooleanBuilder(pool)
	defer b.Release()
	b.AppendValues(data, nil)

	return &Bool{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayBool: b.NewArray().(*arrayBool),
	}
}

type String struct {
	metaField
	*arrayString
}

func NewString(data []string) *String {
	b := array.NewStringBuilder(pool)
	defer b.Release()
	b.AppendValues(data, nil)
	return &String{
		metaField: struct {
			dirt []uint64
		}{
			dirt: []uint64{},
		},
		arrayString: b.NewArray().(*array.String),
	}
}

type HeddleFrame struct {
}

func (h *HeddleFrame) Add(ctx context.Context, a arrow.Array, b arrow.Array, c arrow.Array) error {
	datumA, err := toDatum(a)
	if err != nil {
		return err
	}
	datumB, err := toDatum(b)
	if err != nil {
		return err
	}
	return h.exec(
		ctx,
		"add",
		c,
		datumA,
		datumB,
	)
}

func (h *HeddleFrame) AddScalar(ctx context.Context, a arrow.Array, b float64, c arrow.Array) error {
	datumA, err := toDatum(a)
	if err != nil {
		return err
	}
	return h.exec(
		ctx,
		"add",
		c,
		datumA,
		&compute.ScalarDatum{Value: scalar.NewFloat64Scalar(b)},
	)
}

func (h *HeddleFrame) Subtract(ctx context.Context, a arrow.Array, b arrow.Array, c arrow.Array) error {
	datumA, err := toDatum(a)
	if err != nil {
		return err
	}
	datumB, err := toDatum(b)
	if err != nil {
		return err
	}
	return h.exec(
		ctx,
		"sub",
		c,
		datumA,
		datumB,
	)
}

func (h *HeddleFrame) Multiply(ctx context.Context, a arrow.Array, b float64, c arrow.Array) error {
	datumA, err := toDatum(a)
	if err != nil {
		return err
	}
	return h.exec(
		ctx,
		"multiply",
		c,
		datumA,
		&compute.ScalarDatum{Value: scalar.NewFloat64Scalar(b)},
	)
}

func (h *HeddleFrame) Divide(ctx context.Context, a arrow.Array, b float64, c arrow.Array) error {
	datumA, err := toDatum(a)
	if err != nil {
		return err
	}
	return h.exec(
		ctx,
		"divide",
		c,
		datumA,
		&compute.ScalarDatum{Value: scalar.NewFloat64Scalar(b)},
	)
}

func toDatum(a any) (compute.Datum, error) {
	if a == nil {
		return nil, nil
	}
	if arr, ok := a.(arrow.Array); ok {
		return &compute.ArrayDatum{Value: arr.Data()}, nil
	}
	return nil, fmt.Errorf("convert to datum error - input %T is not an arrow.Array", a)
}

func (h *HeddleFrame) exec(ctx context.Context, name string, output any, inputs ...compute.Datum) error {
	execCtx := compute.DefaultExecCtx()

	outputArrayDatum, err := compute.CallFunction(
		compute.SetExecCtx(ctx, execCtx),
		name,
		nil,
		inputs...,
	)
	if err != nil {
		return err
	}

	values := outputArrayDatum.(*compute.ArrayDatum)
	switch f := output.(type) {
	case *Empty:
		f.arrayInt8 = array.NewInt8Data(values.Value)
	case *Int8:
		f.arrayInt8 = array.NewInt8Data(values.Value)
	case *Int16:
		f.arrayInt16 = array.NewInt16Data(values.Value)
	case *Int32:
		f.arrayInt32 = array.NewInt32Data(values.Value)
	case *Int64:
		f.arrayInt64 = array.NewInt64Data(values.Value)
	case *Uint8:
		f.arrayUint8 = array.NewUint8Data(values.Value)
	case *Uint16:
		f.arrayUint16 = array.NewUint16Data(values.Value)
	case *Uint32:
		f.arrayUint32 = array.NewUint32Data(values.Value)
	case *Uint64:
		f.arrayUint64 = array.NewUint64Data(values.Value)
	case *Float32:
		f.arrayFloat32 = array.NewFloat32Data(values.Value)
	case *Float64:
		f.arrayFloat64 = array.NewFloat64Data(values.Value)
	case *Bool:
		f.arrayBool = array.NewBooleanData(values.Value)
	case *String:
		f.arrayString = array.NewStringData(values.Value)
	}

	return nil
}

type VoidFrame struct {
	HeddleFrame
}
