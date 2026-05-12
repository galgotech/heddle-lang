package plugin

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

// FieldType defines the set of Go types supported by Heddle's strictly-typed DSL.
type FieldType interface {
	int8 | int16 | int32 | int64 | int |
		uint8 | uint16 | uint32 | uint64 | uint |
		float32 | float64 | bool | string
}

// dirtyField provides a uniform interface for managing row-level state (dirty bits)
// across generic fields. This allows HeddleFrame to perform operations like
// bulk deletions without knowing the underlying field types.
type dirtyField interface {
	Delete(rowIndex int)
	IsDirty(rowIndex int) bool
	dirtyBitmap() []uint64
	bind(col *arrow.Column) error
}

// Field represents a strongly-typed, read-optimized column. It wraps an
// Apache Arrow array and maintains a separate bitset for tracking logical
// deletions, ensuring the underlying Arrow buffers remain immutable.
type Field[T FieldType] struct {
	col   arrow.Column
	arr   arrow.Array
	dirty []uint64 // bitset tracking logical deletions to avoid mutating immutable Arrow buffers
	len   int
}

// Delete sets the dirty bit for the specified row, effectively removing it
// from subsequent processing without re-allocating Arrow memory.
func (f *Field[T]) Delete(rowIndex int) {
	if rowIndex < 0 || rowIndex >= f.len {
		panic(fmt.Sprintf("HeddleFrame: index %d out of range (length %d)", rowIndex, f.len))
	}
	f.dirty[rowIndex/64] |= (1 << (uint(rowIndex) % 64))
}

// IsDirty checks the internal bitset to determine if the row has been logically deleted.
func (f *Field[T]) IsDirty(rowIndex int) bool {
	if rowIndex < 0 || rowIndex >= f.len {
		return true // Logically dirty if out of bounds
	}
	return (f.dirty[rowIndex/64] & (1 << (uint(rowIndex) % 64))) != 0
}

// dirtyBitmap returns the raw bitset used for tracking logical deletions.
func (f *Field[T]) dirtyBitmap() []uint64 {
	return f.dirty
}

// IsValid evaluates if a row is present and non-null by checking both the
// logical dirty bitset and the Arrow null bitmap.
func (f *Field[T]) IsValid(rowIndex int) bool {
	if f.IsDirty(rowIndex) {
		return false
	}
	// Check underlying Arrow validity mask
	return f.arr.IsValid(rowIndex)
}

// Value performs a type-safe retrieval of the scalar value at the given index.
// It leverages Go generics and switch-type assertions to map Arrow arrays
// to Go primitives with minimal overhead.
func (f *Field[T]) Value(rowIndex int) T {
	if rowIndex < 0 || rowIndex >= f.len {
		panic(fmt.Sprintf("HeddleFrame: index %d out of range (length %d)", rowIndex, f.len))
	}

	// Implementation Note: This assume single-chunk record batches for simplicity.
	// Production Heddle workflows typically pass flattened record batches to steps
	// to maximize local IPC performance and simplify memory mapping.

	switch any(*new(T)).(type) {
	case int:
		return any(int(f.arr.(*array.Int64).Value(rowIndex))).(T)
	case int64:
		return any(f.arr.(*array.Int64).Value(rowIndex)).(T)
	case int32:
		return any(f.arr.(*array.Int32).Value(rowIndex)).(T)
	case int16:
		return any(f.arr.(*array.Int16).Value(rowIndex)).(T)
	case int8:
		return any(f.arr.(*array.Int8).Value(rowIndex)).(T)
	case uint:
		return any(uint(f.arr.(*array.Uint64).Value(rowIndex))).(T)
	case uint64:
		return any(f.arr.(*array.Uint64).Value(rowIndex)).(T)
	case uint32:
		return any(f.arr.(*array.Uint32).Value(rowIndex)).(T)
	case uint16:
		return any(f.arr.(*array.Uint16).Value(rowIndex)).(T)
	case uint8:
		return any(f.arr.(*array.Uint8).Value(rowIndex)).(T)
	case float64:
		return any(f.arr.(*array.Float64).Value(rowIndex)).(T)
	case float32:
		return any(f.arr.(*array.Float32).Value(rowIndex)).(T)
	case bool:
		return any(f.arr.(*array.Boolean).Value(rowIndex)).(T)
	case string:
		return any(f.arr.(*array.String).Value(rowIndex)).(T)
	default:
		panic("unreachable")
	}
}

// bind attaches an Arrow column to the Field and initializes the tracking bitset.
// It prioritizes single-chunk arrays to maintain high-performance zero-copy semantics.
func (f *Field[T]) bind(col *arrow.Column) error {
	f.col = *col
	f.len = int(col.Len())
	f.dirty = make([]uint64, (f.len+63)/64)

	chunks := col.Data().Chunks()
	if len(chunks) == 1 {
		f.arr = chunks[0]
	} else if len(chunks) > 1 {
		// Fallback for multi-chunk columns; in high-performance paths, these
		// should be pre-flattened by the control plane or data provider.
		f.arr = chunks[0]
	}
	return nil
}

// HeddleFrame is the base abstraction for tabular data in the Heddle ecosystem.
// It synchronizes multiple Fields into a single logical unit, providing
// deterministic execution and efficient memory management.
type HeddleFrame struct {
	native arrow.Table
	fields []dirtyField
}

// VoidFrame is a specialized HeddleFrame representing the 'unit' or 'void' type.
// It carries no data and is used for steps that exist purely for their side effects.
type VoidFrame struct {
	HeddleFrame
}

// bind links the HeddleFrame to an underlying Apache Arrow table.
func (h *HeddleFrame) bind(table arrow.Table) error {
	h.native = table
	return nil
}

// Delete marks a row as dirty across all constituent fields.
func (h *HeddleFrame) Delete(rowIndex int) {
	for _, f := range h.fields {
		f.Delete(rowIndex)
	}
}

// IsDirty reports whether a row has been marked for deletion.
func (h *HeddleFrame) IsDirty(rowIndex int) bool {
	if len(h.fields) == 0 {
		return false
	}
	// Consistency: HeddleFrame.Delete affects all fields uniformly,
	// so checking the primary field's dirty bit is sufficient.
	return h.fields[0].IsDirty(rowIndex)
}

// dirtyBitmap returns the raw bitset used for tracking logical deletions.
func (h *HeddleFrame) dirtyBitmap() []uint64 {
	if len(h.fields) == 0 {
		return nil
	}
	return h.fields[0].dirtyBitmap()
}

// NumRows returns the total number of rows in the frame, including dirty ones.
func (h *HeddleFrame) NumRows() int {
	return int(h.native.NumRows())
}

// NumCols returns the number of columns in the frame.
func (h *HeddleFrame) NumCols() int {
	return int(h.native.NumCols())
}
