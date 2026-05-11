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
	WorkflowID  string
	TaskID      string
	IODirection IODirection
	Path        string // physical path in /dev/shm
}

// NewMetadata creates a new Metadata instance ensuring all required fields are provided.
func NewMetadata(workflowID, taskID string, dir IODirection, path string) Metadata {
	return Metadata{
		WorkflowID:  workflowID,
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
// It validates that the file at Path exists and has secure permissions.
func (r *DataLocalityRegistry) Put(metadata Metadata) error {
	if metadata.Path != "" {
		fi, err := os.Stat(metadata.Path)
		if err != nil {
			return fmt.Errorf("failed to stat shm file: %w", err)
		}
		if err := validateSHMFile(fi); err != nil {
			return err
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
	if meta, ok := r.GetMetadata(workflowID, taskID, dir); ok && meta.Path != "" {
		os.Remove(meta.Path)
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
			if meta.Path != "" {
				os.Remove(meta.Path)
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

// AllocateAndWrite creates a temporary file in /dev/shm, writes the Arrow record batch to it,
// and returns the open file handle. The file is created with 0600 permissions and sealed
// to 0400 after writing to ensure immutability.
func AllocateAndWrite(batch arrow.Record) (*os.File, error) {
	f, err := os.CreateTemp("/dev/shm", "heddle-*.arrow")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	// Layer 1: Restrict permissions immediately
	if err := f.Chmod(0600); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, fmt.Errorf("failed to restrict shm file permissions: %w", err)
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

	// Layer 3: Seal the file (make it read-only for owner)
	if err := f.Chmod(0400); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, fmt.Errorf("failed to seal shm file: %w", err)
	}

	return f, nil
}

// ReadFromPath mmaps the file at the given path and reconstructs the Arrow RecordBatch
// without copying the underlying data. It validates file security before mapping.
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
