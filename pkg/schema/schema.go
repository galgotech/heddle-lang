package schema

import (
	"fmt"
)

// FrameSchemaField represents a single column in a HeddleFrame.
type FrameSchemaField struct {
	Name      string `json:"name"`
	ArrowType string `json:"arrow_type"` // e.g. "int64", "utf8", "bool"
}

// FrameSchema defines the structure of a HeddleFrame.
type FrameSchema struct {
	Fields    []FrameSchemaField `json:"fields"`
	IsVoid    bool               `json:"is_void,omitempty"`
	IsDynamic bool               `json:"is_dynamic,omitempty"`
}

// ConfigField represents a single field in a step configuration.
type ConfigField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ResourceAndConfigSchema defines the structure of a step configuration.
type ResourceAndConfigSchema struct {
	Fields []ConfigField `json:"fields"`
}

// ResourceSchemas contains the config schema for a resource,
// as well as metadata for developer experience (LSP).
type ResourceSchemas struct {
	Config        *ResourceAndConfigSchema `json:"config,omitempty"`
	Documentation string                   `json:"documentation,omitempty"`
	SourceCode    string                   `json:"source_code,omitempty"`
	SourceFile    string                   `json:"source_file,omitempty"`
	SourceLine    int                      `json:"source_line,omitempty"`
}

// StepSchemas contains the input, output and config schemas for a step,
// as well as metadata for developer experience (LSP).
type StepSchemas struct {
	Config        *ResourceAndConfigSchema `json:"config,omitempty"`
	Input         *FrameSchema             `json:"input,omitempty"`
	Output        *FrameSchema             `json:"output,omitempty"`
	Documentation string                   `json:"documentation,omitempty"`
	SourceCode    string                   `json:"source_code,omitempty"`
	SourceFile    string                   `json:"source_file,omitempty"`
	SourceLine    int                      `json:"source_line,omitempty"`
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

	if output.IsDynamic || input.IsDynamic {
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
