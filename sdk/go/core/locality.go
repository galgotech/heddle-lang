//go:build !windows

package core

import (
	"bytes"
	"fmt"
	"os"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"golang.org/x/sys/unix"
)

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

// WriteArrowToShm writes the record batch to a temporary file in /dev/shm and returns the path.
func WriteArrowToShm(batch arrow.Record) (string, error) {
	f, err := os.CreateTemp("/dev/shm", "heddle-plugin-*.arrow")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer f.Close()

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
	// Note: We retain the record because the array points into it, 
	// or we can simply return the column. In arrow-go, arrays are ref-counted.
	// We retain the array and defer releasing the record.
	arr := record.Column(0)
	arr.Retain()
	defer record.Release()
	
	return arr, nil
}

// WriteArrowArrayToShm writes an Arrow Array to a temporary file in /dev/shm.
func WriteArrowArrayToShm(field arrow.Field, arr arrow.Array) (string, error) {
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	record := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer record.Release()

	return WriteArrowToShm(record)
}
