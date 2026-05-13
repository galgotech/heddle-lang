package plugin

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type SchemaTable struct {
	HeddleFrame
	ID     *Int64
	Email  *String
	Active *Bool
}

func TestExtractSchema(t *testing.T) {
	// 1. Extract schema from struct
	s, err := ExtractInputOutputSchema(reflect.TypeFor[*SchemaTable]())
	require.NoError(t, err)
	require.NotNil(t, s)

	// 2. Validate fields
	assert.Equal(t, 3, len(s.Fields))

	// Field 0: ID (with tag)
	assert.Equal(t, "ID", s.Fields[0].Name)
	assert.Equal(t, "int64", s.Fields[0].ArrowType)

	// Field 1: Email (with tag)
	assert.Equal(t, "Email", s.Fields[1].Name)
	assert.Equal(t, "utf8", s.Fields[1].ArrowType)

	// Field 2: Active (no tag)
	assert.Equal(t, "Active", s.Fields[2].Name)
	assert.Equal(t, "bool", s.Fields[2].ArrowType)
}

func TestExtractConfigSchema(t *testing.T) {
	type ConfigTest struct {
		Config
		Name    string `json:"name"`
		Timeout int    `json:"timeout"`
		Hidden  string `json:"-"`
	}

	s, err := ExtractResourceAndConfigSchema(reflect.TypeFor[ConfigTest]())
	require.NoError(t, err)
	require.NotNil(t, s)

	assert.Equal(t, 2, len(s.Fields))
	assert.Equal(t, "name", s.Fields[0].Name)
	assert.Equal(t, "string", s.Fields[0].Type)
	assert.Equal(t, "timeout", s.Fields[1].Name)
	assert.Equal(t, "int", s.Fields[1].Type)
}
