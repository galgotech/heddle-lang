package plugin

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/galgotech/heddle-lang/pkg/schema"
)

// ExtractResourceAndConfigSchema performs type introspection to derive a basic JSON schema for Step/Resource configurations.
func ExtractResourceAndConfigSchema(t reflect.Type) (*schema.ResourceAndConfigSchema, error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("ExtractConfigSchema: expected struct, got %s", t.Kind())
	}

	res := &schema.ResourceAndConfigSchema{
		Fields: []schema.ConfigField{},
	}

	for field := range t.Fields() {
		if field.Type == reflect.TypeFor[Config]() {
			continue
		}
		jsonTag := field.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}
		name := jsonTag
		if name == "" {
			name = field.Name
		} else {
			// Handle cases like json:"name,omitempty"
			parts := strings.Split(name, ",")
			name = parts[0]
			if name == "" {
				name = field.Name
			}
		}

		res.Fields = append(res.Fields, schema.ConfigField{
			Name: name,
			Type: field.Type.Kind().String(),
		})
	}
	return res, nil
}

// ExtractInputOutputSchema uses reflection to derive a Heddle schema from a reflect.Type (expected to be a struct).
func ExtractInputOutputSchema(t reflect.Type) (*schema.FrameSchema, error) {
	if t == nil {
		return nil, fmt.Errorf("ExtractSchema: nil type or generic interface not allowed; must use a struct embedding HeddleFrame")
	}

	if t.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("ExtractSchema: expected pointer to struct, got %s", t.Kind())
	}

	t = t.Elem()
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("ExtractSchema: expected struct embedding HeddleFrame, got %s", t.Kind())
	}

	if t == reflect.TypeFor[VoidFrame]() {
		return &schema.FrameSchema{IsVoid: true}, nil
	}

	// Verify it embeds HeddleFrame
	hasFrame := false
	for field := range t.Fields() {
		if field.Type == reflect.TypeFor[HeddleFrame]() {
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

	for f := range t.Fields() {
		fieldType := f.Type
		if fieldType == reflect.TypeFor[HeddleFrame]() {
			continue
		}

		if fieldType.Kind() != reflect.Pointer {
			return nil, fmt.Errorf("ExtractSchema: expected pointer to HeddleFrame field, got %s", fieldType.Kind())
		}

		fieldType = fieldType.Elem()

		var arrowType string
		switch fieldType {
		case reflect.TypeFor[Int8]():
			arrowType = "int8"
		case reflect.TypeFor[Int16]():
			arrowType = "int16"
		case reflect.TypeFor[Int32]():
			arrowType = "int32"
		case reflect.TypeFor[Int64]():
			arrowType = "int64"
		case reflect.TypeFor[Uint8]():
			arrowType = "uint8"
		case reflect.TypeFor[Uint16]():
			arrowType = "uint16"
		case reflect.TypeFor[Uint32]():
			arrowType = "uint32"
		case reflect.TypeFor[Uint64]():
			arrowType = "uint64"
		case reflect.TypeFor[Float32]():
			arrowType = "float32"
		case reflect.TypeFor[Float64]():
			arrowType = "float64"
		case reflect.TypeFor[Bool]():
			arrowType = "bool"
		case reflect.TypeFor[String]():
			arrowType = "utf8"
		default:
			continue
		}

		res.Fields = append(res.Fields, schema.FrameSchemaField{
			Name:      f.Name,
			ArrowType: arrowType,
		})
	}

	return res, nil
}
