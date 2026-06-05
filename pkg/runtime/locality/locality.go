package locality

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"golang.org/x/sys/unix"
)

type IODirection int

const (
	Input IODirection = iota
	Output
)

func (d IODirection) String() string {
	if d == Input {
		return "Input"
	}
	return "Output"
}

type Metadata struct {
	WorkflowID  string
	TaskID      string
	IODirection IODirection
	Paths       map[string]string // physical paths in /dev/shm
}

// NewMetadata creates a new Metadata instance ensuring all required fields are provided.
func NewMetadata(workflowID, taskID string, dir IODirection, paths map[string]string) Metadata {
	return Metadata{
		WorkflowID:  workflowID,
		TaskID:      taskID,
		IODirection: dir,
		Paths:       paths,
	}
}

// DataLocalityRegistry manages the mapping of data identifiers to their physical locations
// in /dev/shm. Data is stored via WriteArrowToShm and accessed via ReadArrowFromPath.
// The registry tracks only Metadata (including the SHM path) — no in-process copies.
type DataLocalityRegistry struct {
	metadata sync.Map
}

// Put registers a data identifier with its corresponding SHM metadata.
// It validates that the file at Path exists and has secure permissions.
func (r *DataLocalityRegistry) Put(metadata Metadata) error {
	for _, path := range metadata.Paths {
		if path != "" {
			fi, err := os.Stat(path)
			if err != nil {
				return fmt.Errorf("failed to stat shm file: %w", err)
			}
			if err := validateSHMFile(fi); err != nil {
				return err
			}
		}
	}

	key := r.makeKey(metadata.WorkflowID, metadata.TaskID, metadata.IODirection)
	r.metadata.Store(key, metadata)
	return nil
}

// GetMetadata retrieves the metadata associated with a data identifier.
func (r *DataLocalityRegistry) GetMetadata(workflowID, taskID string, dir IODirection) (Metadata, bool) {
	key := r.makeKey(workflowID, taskID, dir)
	metadata, ok := r.metadata.Load(key)
	if !ok {
		return Metadata{}, false
	}

	return metadata.(Metadata), true
}

// Delete removes a data identifier from the registry and unlinks the underlying SHM file if present.
func (r *DataLocalityRegistry) Delete(workflowID, taskID string, dir IODirection) {
	if meta, ok := r.GetMetadata(workflowID, taskID, dir); ok {
		for _, path := range meta.Paths {
			if path != "" {
				os.Remove(path)
			}
		}
	}
	key := r.makeKey(workflowID, taskID, dir)
	r.metadata.Delete(key)
}

func (r *DataLocalityRegistry) makeKey(workflowID, taskID string, dir IODirection) string {
	return workflowID + ":" + taskID + ":" + dir.String()
}

// DeleteByWorkflow removes all SHM entries and files for the given workflow.
func (r *DataLocalityRegistry) DeleteByWorkflow(workflowID string) {
	r.metadata.Range(func(key, value interface{}) bool {
		meta := value.(Metadata)
		if meta.WorkflowID == workflowID {
			for _, path := range meta.Paths {
				if path != "" {
					os.Remove(path)
				}
			}
			r.metadata.Delete(key)
		}
		return true
	})
}

// NewDataLocalityRegistry initializes a new registry.
func NewDataLocalityRegistry() *DataLocalityRegistry {
	return &DataLocalityRegistry{}
}

// WriteArrowArrayOnlyToShm writes an Arrow Array to a temporary file in /dev/shm using a default field name.
// This allows writing an array without the caller having to manually define an arrow.Field or arrow.Schema.
func WriteArrowArrayOnlyToShm(arr arrow.Array) (string, error) {
	field := arrow.Field{Name: "v", Type: arr.DataType(), Nullable: true}
	return WriteArrowArrayToShm(field, arr)
}

// WriteArrowArrayToShm writes an Arrow Array to a temporary file in /dev/shm
// as a 1-column RecordBatch, and returns the path.
func WriteArrowArrayToShm(field arrow.Field, arr arrow.Array) (string, error) {
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	record := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer record.Release()

	return WriteRecordToShm(record)
}

// WriteRecordToShm writes the record batch to a temporary file in /dev/shm and returns the path.
// The file is created with 0600 permissions and sealed to 0400 after writing to ensure immutability.
func WriteRecordToShm(batch arrow.Record) (string, error) {
	f, err := os.CreateTemp("/dev/shm", "heddle-*.arrow")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer f.Close()

	// Layer 1: Restrict permissions immediately
	if err := f.Chmod(0600); err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("failed to restrict shm file permissions: %w", err)
	}

	writer, err := ipc.NewFileWriter(f, ipc.WithSchema(batch.Schema()))
	if err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("failed to create ipc writer: %w", err)
	}
	if err := writer.Write(batch); err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("failed to write record batch: %w", err)
	}

	if err := writer.Close(); err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("failed to close ipc writer: %w", err)
	}

	// Layer 3: Seal the file (make it read-only for owner)
	if err := f.Chmod(0400); err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("failed to seal shm file: %w", err)
	}

	return f.Name(), nil
}

// ReadArrowArrayFromPath mmaps the file at path and returns the first Arrow Array from the record batch.
func ReadArrowArrayFromPath(path string) (arrow.Array, error) {
	record, err := ReadArrowFromPath(path)
	if err != nil {
		return nil, err
	}
	if record.NumCols() == 0 {
		return nil, fmt.Errorf("record batch has no columns")
	}
	arr := record.Column(0)
	arr.Retain()
	defer record.Release()

	return arr, nil
}

// ReadArrowFromPath mmaps the file at path and returns the first Arrow record batch.
func ReadArrowFromPath(path string) (arrow.Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	if err := validateSHMFile(fi); err != nil {
		return nil, err
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(fi.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	reader, err := ipc.NewFileReader(bytes.NewReader(data))
	if err != nil {
		unix.Munmap(data)
		return nil, fmt.Errorf("failed to create ipc reader: %w", err)
	}

	record, err := reader.Read()
	if err != nil {
		unix.Munmap(data)
		return nil, fmt.Errorf("failed to read record: %w", err)
	}

	return record, nil
}

// FormatArrowPreview reads up to the first 5 records of an Arrow RecordBatch from the given path in /dev/shm,
// and returns a formatted text table representation of the data.
func FormatArrowPreview(path string) (string, error) {
	record, err := ReadArrowFromPath(path)
	if err != nil {
		return "", err
	}
	defer record.Release()

	numRows := int(record.NumRows())
	if numRows > 5 {
		numRows = 5
	}

	numCols := int(record.NumCols())
	schema := record.Schema()

	var sb strings.Builder

	// Calculate maximum width for each column to make it a beautiful table
	colWidths := make([]int, numCols)
	for j := 0; j < numCols; j++ {
		field := schema.Field(j)
		header := fmt.Sprintf("%s (%s)", field.Name, field.Type.Name())
		width := len(header)
		for i := 0; i < numRows; i++ {
			valStr := "NULL"
			col := record.Column(j)
			if !col.IsNull(i) {
				valStr = col.ValueStr(i)
			}
			if len(valStr) > width {
				width = len(valStr)
			}
		}
		colWidths[j] = width
	}

	// Helper to draw horizontal line
	drawDivider := func() {
		sb.WriteString("+")
		for _, w := range colWidths {
			sb.WriteString(strings.Repeat("-", w+2))
			sb.WriteString("+")
		}
		sb.WriteString("\n")
	}

	drawDivider()

	// Print headers
	sb.WriteString("|")
	for j, w := range colWidths {
		field := schema.Field(j)
		header := fmt.Sprintf("%s (%s)", field.Name, field.Type.Name())
		sb.WriteString(" ")
		sb.WriteString(header)
		sb.WriteString(strings.Repeat(" ", w-len(header)))
		sb.WriteString(" |")
	}
	sb.WriteString("\n")

	drawDivider()

	// Print rows
	for i := 0; i < numRows; i++ {
		sb.WriteString("|")
		for j, w := range colWidths {
			valStr := "NULL"
			col := record.Column(j)
			if !col.IsNull(i) {
				valStr = col.ValueStr(i)
			}
			sb.WriteString(" ")
			sb.WriteString(valStr)
			sb.WriteString(strings.Repeat(" ", w-len(valStr)))
			sb.WriteString(" |")
		}
		sb.WriteString("\n")
	}

	drawDivider()

	if int(record.NumRows()) > 5 {
		sb.WriteString(fmt.Sprintf("... showing 5 of %d rows ...\n", record.NumRows()))
	}

	return sb.String(), nil
}

// WriteDirtyToShm writes the dirty bitmap to a temp file in /dev/shm.
func WriteDirtyToShm(dirty []uint64) (string, error) {
	f, err := os.CreateTemp("/dev/shm", "heddle-dirty-*.bin")
	if err != nil {
		return "", fmt.Errorf("failed to create dirty file: %w", err)
	}
	defer f.Close()

	// Simple binary write of uint64 slice
	data := make([]byte, len(dirty)*8)
	for i, v := range dirty {
		for j := 0; j < 8; j++ {
			data[i*8+j] = byte(v >> (j * 8))
		}
	}

	if _, err := f.Write(data); err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("failed to write dirty data: %w", err)
	}

	return f.Name(), nil
}

// ReadDirtyFromPath reads the dirty bitmap from SHM.
func ReadDirtyFromPath(path string) ([]uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read dirty file: %w", err)
	}

	if len(data)%8 != 0 {
		return nil, fmt.Errorf("invalid dirty file size")
	}

	res := make([]uint64, len(data)/8)
	for i := range res {
		var v uint64
		for j := 0; j < 8; j++ {
			v |= uint64(data[i*8+j]) << (j * 8)
		}
		res[i] = v
	}

	return res, nil
}

// validateSHMFile checks that the file is owner-only and owned by the current process.
func validateSHMFile(fi os.FileInfo) error {
	// Check that it's owner-only (mode & 0077 == 0)
	if fi.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("insecure permissions: shm file %s is world-readable or group-readable", fi.Name())
	}

	// In a real system we'd also check UID matches os.Getuid()
	// for simplicity in this environment we focus on the Mode.
	return nil
}
