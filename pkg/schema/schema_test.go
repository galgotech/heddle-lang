package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompatible(t *testing.T) {
	tests := []struct {
		name    string
		output  []ColumnSchema
		input   []ColumnSchema
		wantErr bool
	}{
		{
			name: "Matching non-void schemas are compatible",
			output: []ColumnSchema{
				{Name: "id", ArrowType: "int64"},
			},
			input: []ColumnSchema{
				{Name: "id", ArrowType: "int64"},
			},
		},
		{
			name: "Type mismatch is incompatible",
			output: []ColumnSchema{
				{Name: "id", ArrowType: "int64"},
			},
			input: []ColumnSchema{
				{Name: "id", ArrowType: "utf8"},
			},
			wantErr: true,
		},
		{
			name: "Field count mismatch is incompatible",
			output: []ColumnSchema{
				{Name: "id", ArrowType: "int64"},
			},
			input: []ColumnSchema{
				{Name: "id", ArrowType: "int64"},
				{Name: "name", ArrowType: "utf8"},
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
