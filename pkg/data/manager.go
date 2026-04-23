package data

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/ipc"
)

// DataManager handles shared memory allocation and Arrow IPC data exchange.
type DataManager struct {
	basePath string
}

// NewDataManager creates a new DataManager.
func NewDataManager(basePath string) *DataManager {
	// Ensure the base path exists
	_ = os.MkdirAll(basePath, 0777)
	return &DataManager{basePath: basePath}
}

// Put writes an Arrow Record to shared memory.
func (m *DataManager) Put(id string, record arrow.Record) error {
	path := filepath.Join(m.basePath, id)

	// 1. Serialize record to a buffer first to know the size
	// (In a more optimized version, we'd pre-calculate or use a streaming approach)
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record to buffer: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	data := buf.Bytes()
	size := int64(len(data))

	// 2. Create the shared memory file
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to create shm file: %w", err)
	}
	defer f.Close()

	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("failed to truncate shm file: %w", err)
	}

	// 3. Mmap the file and copy data
	mmap, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap shm file: %w", err)
	}
	defer syscall.Munmap(mmap)

	copy(mmap, data)

	return nil
}

// Get reads an Arrow Record from shared memory.
func (m *DataManager) Get(id string) (arrow.Record, error) {
	path := filepath.Join(m.basePath, id)

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open shm file: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat shm file: %w", err)
	}

	size := fi.Size()
	if size == 0 {
		return nil, fmt.Errorf("shm file is empty")
	}

	// Mmap the file for reading
	mmap, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap shm file for reading: %w", err)
	}

	reader, err := ipc.NewReader(bytes.NewReader(mmap))
	if err != nil {
		syscall.Munmap(mmap)
		return nil, fmt.Errorf("failed to create ipc reader: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		syscall.Munmap(mmap)
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("failed to read record: %w", err)
		}
		return nil, fmt.Errorf("no record found in shm file")
	}

	rec := reader.Record()
	rec.Retain() // Retain because reader.Release() will release it otherwise

	syscall.Munmap(mmap)
	return rec, nil
}

// Cleanup removes all shared memory resources managed by this instance.
func (m *DataManager) Cleanup() {
	_ = os.RemoveAll(m.basePath)
}
