package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompatible(t *testing.T) {
	tests := []struct {
		name    string
		output  *FrameSchema
		input   *FrameSchema
		wantErr bool
	}{
		{
			name:   "Nil schemas are compatible",
			output: nil,
			input:  nil,
		},
		{
			name:   "Void schemas are compatible",
			output: &FrameSchema{IsVoid: true},
			input:  &FrameSchema{IsVoid: true},
		},
		{
			name:    "Void and non-void are incompatible",
			output:  &FrameSchema{IsVoid: true},
			input:   &FrameSchema{IsVoid: false},
			wantErr: true,
		},
		{
			name: "Matching non-void schemas are compatible",
			output: &FrameSchema{
				Fields: []FrameSchemaField{
					{Name: "id", ArrowType: "int64"},
				},
			},
			input: &FrameSchema{
				Fields: []FrameSchemaField{
					{Name: "id", ArrowType: "int64"},
				},
			},
		},
		{
			name: "Type mismatch is incompatible",
			output: &FrameSchema{
				Fields: []FrameSchemaField{
					{Name: "id", ArrowType: "int64"},
				},
			},
			input: &FrameSchema{
				Fields: []FrameSchemaField{
					{Name: "id", ArrowType: "utf8"},
				},
			},
			wantErr: true,
		},
		{
			name: "Field count mismatch is incompatible",
			output: &FrameSchema{
				Fields: []FrameSchemaField{
					{Name: "id", ArrowType: "int64"},
				},
			},
			input: &FrameSchema{
				Fields: []FrameSchemaField{
					{Name: "id", ArrowType: "int64"},
					{Name: "name", ArrowType: "utf8"},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Compatible(tt.output, tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
