package schema

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
)

// FrameSchemaField represents a single column in a HeddleFrame.
type FrameSchemaField struct {
	Name      string `json:"name"`
	ArrowType string `json:"arrow_type"` // e.g. "int64", "utf8", "bool"
	Nullable  bool   `json:"nullable"`
}

// FrameSchema defines the structure of a HeddleFrame.
type FrameSchema struct {
	Fields []FrameSchemaField `json:"fields"`
	IsVoid bool               `json:"is_void,omitempty"`
}

// ToArrowSchema converts a FrameSchema to an arrow.Schema.
func (s *FrameSchema) ToArrowSchema() (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(s.Fields))
	for i, f := range s.Fields {
		var dt arrow.DataType
		switch f.ArrowType {
		case "int64":
			dt = arrow.PrimitiveTypes.Int64
		case "int32":
			dt = arrow.PrimitiveTypes.Int32
		case "int16":
			dt = arrow.PrimitiveTypes.Int16
		case "int8":
			dt = arrow.PrimitiveTypes.Int8
		case "uint64":
			dt = arrow.PrimitiveTypes.Uint64
		case "uint32":
			dt = arrow.PrimitiveTypes.Uint32
		case "uint16":
			dt = arrow.PrimitiveTypes.Uint16
		case "uint8":
			dt = arrow.PrimitiveTypes.Uint8
		case "float64":
			dt = arrow.PrimitiveTypes.Float64
		case "float32":
			dt = arrow.PrimitiveTypes.Float32
		case "bool":
			dt = arrow.FixedWidthTypes.Boolean
		case "utf8":
			dt = arrow.BinaryTypes.String
		default:
			return nil, fmt.Errorf("unsupported arrow type: %s", f.ArrowType)
		}
		fields[i] = arrow.Field{Name: f.Name, Type: dt, Nullable: f.Nullable}
	}
	return arrow.NewSchema(fields, nil), nil
}

// StepSchemas contains the input and output schemas for a step.
type StepSchemas struct {
	Input  *FrameSchema `json:"input,omitempty"`
	Output *FrameSchema `json:"output,omitempty"`
}

// Compatible checks if the output schema of one step is compatible with
// the input schema of the next. Returns nil if compatible.
func Compatible(output, input *FrameSchema) error {
	if output == nil || input == nil {
		return nil
	}

	if output.IsVoid != input.IsVoid {
		return fmt.Errorf("void mismatch: output is void=%v, input is void=%v", output.IsVoid, input.IsVoid)
	}

	if output.IsVoid {
		return nil
	}

	if len(output.Fields) != len(input.Fields) {
		return fmt.Errorf("schema mismatch: output has %d fields, input has %d fields", len(output.Fields), len(input.Fields))
	}

	for i := range output.Fields {
		of := output.Fields[i]
		inf := input.Fields[i]
		if of.ArrowType != inf.ArrowType {
			return fmt.Errorf("type mismatch at field %d (%s): output is %s, input is %s", i, of.Name, of.ArrowType, inf.ArrowType)
		}
	}

	return nil
}
