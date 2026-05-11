package locality

import (
	"bytes"
	"fmt"
	"os"
	"sync"

	"github.com/apache/arrow/go/v18/arrow"
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
	TaskID      string
	IODirection IODirection
	Path        string // physical path in /dev/shm
}

// NewMetadata creates a new Metadata instance ensuring all required fields are provided.
func NewMetadata(taskID string, dir IODirection, path string) Metadata {
	return Metadata{
		TaskID:      taskID,
		IODirection: dir,
		Path:        path,
	}
}

// DataLocalityRegistry manages the mapping of data identifiers to their physical locations
// in /dev/shm. Data is stored via AllocateAndWrite and accessed via ReadFromPath.
// The registry tracks only Metadata (including the SHM path) — no in-process copies.
type DataLocalityRegistry struct {
	metadata sync.Map
}

// Put registers a data identifier with its corresponding SHM metadata.
func (r *DataLocalityRegistry) Put(metadata Metadata) {
	key := metadata.TaskID + metadata.IODirection.String()
	r.metadata.Store(key, metadata)
}

// GetMetadata retrieves the metadata associated with a data identifier.
func (r *DataLocalityRegistry) GetMetadata(taskID string, dir IODirection) (Metadata, bool) {
	key := taskID + dir.String()
	metadata, ok := r.metadata.Load(key)
	if !ok {
		return Metadata{}, false
	}

	return metadata.(Metadata), true
}

// Delete removes a data identifier from the registry and unlinks the underlying SHM file if present.
func (r *DataLocalityRegistry) Delete(taskID string, dir IODirection) {
	if meta, ok := r.GetMetadata(taskID, dir); ok && meta.Path != "" {
		os.Remove(meta.Path)
	}
	key := taskID + dir.String()
	r.metadata.Delete(key)
}

// NewDataLocalityRegistry initializes a new registry.
func NewDataLocalityRegistry() *DataLocalityRegistry {
	return &DataLocalityRegistry{}
}

// AllocateAndWrite creates a temporary file in /dev/shm, writes the Arrow record batch to it,
// and returns the open file handle. The caller is responsible for unlinking the file.
func AllocateAndWrite(batch arrow.Record) (*os.File, error) {
	f, err := os.CreateTemp("/dev/shm", "heddle-*.arrow")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	writer, err := ipc.NewFileWriter(f, ipc.WithSchema(batch.Schema()))
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, fmt.Errorf("failed to create ipc writer: %w", err)
	}
	if err := writer.Write(batch); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, fmt.Errorf("failed to write record batch: %w", err)
	}

	if err := writer.Close(); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, fmt.Errorf("failed to close ipc writer: %w", err)
	}

	return f, nil
}

// ReadFromPath mmaps the file at the given path and reconstructs the Arrow RecordBatch
// without copying the underlying data.
func ReadFromPath(path string) (arrow.Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
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

// Unlink closes the file and removes it from the filesystem.
func Unlink(f *os.File) error {
	name := f.Name()
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}
	if err := os.Remove(name); err != nil {
		return fmt.Errorf("failed to remove file: %w", err)
	}
	return nil
}
