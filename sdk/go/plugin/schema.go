package plugin

import (
	"fmt"
	"reflect"

	"github.com/galgotech/heddle-lang/pkg/schema"
)

// ExtractSchema uses reflection to derive a Heddle schema from a reflect.Type (expected to be a struct).
func ExtractSchema(t reflect.Type) (*schema.FrameSchema, error) {
	if t == nil {
		return nil, fmt.Errorf("ExtractSchema: nil type or generic interface not allowed; must use a struct embedding HeddleFrame")
	}

	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("ExtractSchema: expected struct embedding HeddleFrame, got %s", t.Kind())
	}

	if t == reflect.TypeFor[VoidFrame]() {
		return &schema.FrameSchema{IsVoid: true}, nil
	}

	// Verify it embeds HeddleFrame
	hasFrame := false
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Type == reflect.TypeOf(HeddleFrame{}) {
			hasFrame = true
			break
		}
	}
	if !hasFrame {
		return nil, fmt.Errorf("ExtractSchema: type %s does not embed HeddleFrame", t.Name())
	}

	res := &schema.FrameSchema{
		Fields: []schema.FrameSchemaField{},
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		// Check if it's a Field[T]
		// We can check if it implements dirtyField (but it's internal)
		// Or check the type name/structure.
		// Since Field[T] is a struct with a bind method, we check for that pattern.

		if f.Type.Kind() == reflect.Pointer {
			name := f.Tag.Get("heddle")
			if name == "" {
				name = f.Name
			}

			var arrowType string
			switch f.Type {
			case reflect.TypeFor[*Int8]():
				arrowType = "int8"
			case reflect.TypeFor[*Int16]():
				arrowType = "int16"
			case reflect.TypeFor[*Int32]():
				arrowType = "int32"
			case reflect.TypeFor[*Int64]():
				arrowType = "int64"
			case reflect.TypeFor[*Uint8]():
				arrowType = "uint8"
			case reflect.TypeFor[*Uint16]():
				arrowType = "uint16"
			case reflect.TypeFor[*Uint32]():
				arrowType = "uint32"
			case reflect.TypeFor[*Uint64]():
				arrowType = "uint64"
			case reflect.TypeFor[*Float32]():
				arrowType = "float32"
			case reflect.TypeFor[*Float64]():
				arrowType = "float64"
			case reflect.TypeFor[*Bool]():
				arrowType = "bool"
			case reflect.TypeFor[*String]():
				arrowType = "utf8"
			default:
				continue
			}

			res.Fields = append(res.Fields, schema.FrameSchemaField{
				Name:      name,
				ArrowType: arrowType,
				Nullable:  true,
			})
		}
	}

	return res, nil
}

func goTypeToArrowType(t reflect.Type) (string, error) {
	switch t.Kind() {
	case reflect.Int, reflect.Int64:
		return "int64", nil
	case reflect.Int32:
		return "int32", nil
	case reflect.Int16:
		return "int16", nil
	case reflect.Int8:
		return "int8", nil
	case reflect.Uint64, reflect.Uint:
		return "uint64", nil
	case reflect.Uint32:
		return "uint32", nil
	case reflect.Uint16:
		return "uint16", nil
	case reflect.Uint8:
		return "uint8", nil
	case reflect.Float64:
		return "float64", nil
	case reflect.Float32:
		return "float32", nil
	case reflect.Bool:
		return "bool", nil
	case reflect.String:
		return "utf8", nil
	default:
		return "", fmt.Errorf("unsupported type for HeddleField: %s", t.String())
	}
}

// Extension method for the schema type for convenience in the SDK
type FrameSchema = schema.FrameSchema
