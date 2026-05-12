package plugin_test

import (
	"reflect"
	"testing"

	"github.com/galgotech/heddle-lang/sdk/go/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type SchemaTable struct {
	plugin.HeddleFrame
	ID     *plugin.Int64  `heddle:"id"`
	Email  *plugin.String `heddle:"user_email"`
	Active *plugin.Bool
}

func TestExtractSchema(t *testing.T) {
	// 1. Extract schema from struct
	s, err := plugin.ExtractSchema(reflect.TypeOf(SchemaTable{}))
	require.NoError(t, err)
	require.NotNil(t, s)

	// 2. Validate fields
	assert.Equal(t, 3, len(s.Fields))

	// Field 0: ID (with tag)
	assert.Equal(t, "id", s.Fields[0].Name)
	assert.Equal(t, "int64", s.Fields[0].ArrowType)

	// Field 1: Email (with tag)
	assert.Equal(t, "user_email", s.Fields[1].Name)
	assert.Equal(t, "utf8", s.Fields[1].ArrowType)

	// Field 2: Active (no tag)
	assert.Equal(t, "Active", s.Fields[2].Name)
	assert.Equal(t, "bool", s.Fields[2].ArrowType)
}

func TestToArrowSchema(t *testing.T) {
	s, _ := plugin.ExtractSchema(reflect.TypeOf(SchemaTable{}))
	arrowSchema, err := s.ToArrowSchema()
	require.NoError(t, err)
	require.NotNil(t, arrowSchema)

	assert.Equal(t, 3, arrowSchema.NumFields())
	assert.Equal(t, "id", arrowSchema.Field(0).Name)
	assert.Equal(t, "user_email", arrowSchema.Field(1).Name)
	assert.Equal(t, "Active", arrowSchema.Field(2).Name)
}
