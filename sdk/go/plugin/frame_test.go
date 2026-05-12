package plugin

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type UserTable struct {
	HeddleFrame
	ID       Field[int64]  `heddle:"id"`
	UserName Field[string] `heddle:"username"`
	Active   Field[bool]
}

func TestHeddleFrameAndField(t *testing.T) {
	pool := memory.NewGoAllocator()

	// 1. Build an Arrow table
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "username", Type: arrow.BinaryTypes.String},
		{Name: "Active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	ib := array.NewInt64Builder(pool)
	defer ib.Release()
	sb := array.NewStringBuilder(pool)
	defer sb.Release()
	bb := array.NewBooleanBuilder(pool)
	defer bb.Release()

	ib.AppendValues([]int64{1, 2, 3}, nil)
	sb.AppendValues([]string{"alice", "bob", "charlie"}, nil)
	bb.AppendValues([]bool{true, false, true}, nil)

	ic := ib.NewArray()
	defer ic.Release()
	sc := sb.NewArray()
	defer sc.Release()
	bc := bb.NewArray()
	defer bc.Release()

	// Convert to table
	tbl := array.NewTable(schema, []arrow.Column{
		*arrow.NewColumn(schema.Field(0), arrow.NewChunked(schema.Field(0).Type, []arrow.Array{ic})),
		*arrow.NewColumn(schema.Field(1), arrow.NewChunked(schema.Field(1).Type, []arrow.Array{sc})),
		*arrow.NewColumn(schema.Field(2), arrow.NewChunked(schema.Field(2).Type, []arrow.Array{bc})),
	}, 3)
	defer tbl.Release()

	// 2. Bind to UserTable using internal bindFrameValue logic
	ut := &UserTable{}
	err := bindFrameValue(reflect.ValueOf(ut).Elem(), tbl)
	require.NoError(t, err)

	// 3. Test values
	assert.Equal(t, int64(1), ut.ID.Value(0))
	assert.Equal(t, "alice", ut.UserName.Value(0))
	assert.True(t, ut.Active.Value(0))

	assert.Equal(t, int64(2), ut.ID.Value(1))
	assert.Equal(t, "bob", ut.UserName.Value(1))
	assert.False(t, ut.Active.Value(1))

	// 4. Test Soft Delete (Cell)
	assert.True(t, ut.UserName.IsValid(1))
	ut.UserName.Delete(1)
	assert.False(t, ut.UserName.IsValid(1))
	assert.True(t, ut.ID.IsValid(1)) // ID still valid

	// 5. Test Soft Delete (Row)
	assert.True(t, ut.ID.IsValid(2))
	assert.True(t, ut.UserName.IsValid(2))
	ut.Delete(2)
	assert.False(t, ut.ID.IsValid(2))
	assert.False(t, ut.UserName.IsValid(2))
	assert.False(t, ut.Active.IsValid(2))

	// Row 0 should still be valid
	assert.True(t, ut.ID.IsValid(0))
}

func TestOOBPanic(t *testing.T) {
	// ... (implementation of OOB test)
}
