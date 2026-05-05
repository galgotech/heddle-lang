package core

import (
	"bytes"
	"fmt"
	"os"
	"syscall"

	"github.com/apache/arrow/go/v18/arrow/ipc"
)

// IsAbsolutePath checks if a path is absolute.
func IsAbsolutePath(path string) bool {
	return len(path) > 0 && path[0] == '/'
}

// GetSharedMemoryPath returns the full path for a Heddle SHM handle.
func GetSharedMemoryPath(handle string) string {
	return "/dev/shm/heddle/" + handle
}

// ReadTableFromHandle reads an Arrow Record from a file handle (e.g., in SHM)
// and returns a Table wrapping it. This uses mmap for zero-copy reading.
func ReadTableFromHandle(handle string) (Table, error) {
	f, err := os.Open(handle)
	if err != nil {
		return nil, fmt.Errorf("failed to open handle %s: %w", handle, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat handle %s: %w", handle, err)
	}

	size := fi.Size()
	if size == 0 {
		return nil, fmt.Errorf("handle %s is empty", handle)
	}

	// Mmap the file for zero-copy access
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap handle %s: %w", handle, err)
	}
	defer syscall.Munmap(data)

	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader for %s: %w", handle, err)
	}
	defer reader.Release()

	if !reader.Next() {
		return nil, fmt.Errorf("no records found in handle %s", handle)
	}

	record := reader.Record()
	record.Retain()

	return NewTableFromRecord(record), nil
}

// WriteTableToHandle writes an Arrow Record to a file handle (e.g., in SHM)
// using the Arrow IPC format.
func WriteTableToHandle(handle string, table Table) error {
	if table == nil || table.Native() == nil {
		return fmt.Errorf("cannot write nil table or record")
	}

	f, err := os.OpenFile(handle, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to open handle %s for writing: %w", handle, err)
	}
	defer f.Close()

	writer := ipc.NewWriter(f, ipc.WithSchema(table.Native().Schema()))
	if err := writer.Write(table.Native()); err != nil {
		return fmt.Errorf("failed to write record to %s: %w", handle, err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close arrow writer for %s: %w", handle, err)
	}

	return nil
}

// ReadTableFromFD reads an Arrow Record from a file descriptor using mmap.
func ReadTableFromFD(fd int) (Table, error) {
	// 1. Get file size
	size, err := syscall.Seek(fd, 0, 2) // io.SeekEnd
	if err != nil {
		return nil, fmt.Errorf("failed to seek FD: %w", err)
	}
	if _, err := syscall.Seek(fd, 0, 0); err != nil { // io.SeekStart
		return nil, fmt.Errorf("failed to reset FD: %w", err)
	}

	if size == 0 {
		return nil, fmt.Errorf("FD is empty")
	}

	// 2. Mmap the file
	data, err := syscall.Mmap(fd, 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap FD: %w", err)
	}
	defer syscall.Munmap(data)

	// 3. Create Arrow reader
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader from FD: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		return nil, fmt.Errorf("no records found in FD")
	}

	record := reader.Record()
	record.Retain()

	return NewTableFromRecord(record), nil
}
