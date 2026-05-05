package data

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// osMemoryRegion implements MemoryRegion using POSIX mmap.
type osMemoryRegion struct {
	data []byte
	file *os.File
}

func (r *osMemoryRegion) Data() []byte { return r.data }
func (r *osMemoryRegion) File() *os.File { return r.file }
func (r *osMemoryRegion) Unmap() error {
	if r.data != nil {
		err := syscall.Munmap(r.data)
		r.data = nil
		return err
	}
	return nil
}

// OSMemoryAllocator implements MemoryAllocator using host OS syscalls.
type OSMemoryAllocator struct {
	basePath string
}

// NewOSMemoryAllocator initializes an allocator with a specific base path for disk-backed files.
func NewOSMemoryAllocator(basePath string) *OSMemoryAllocator {
	return &OSMemoryAllocator{basePath: basePath}
}

func (a *OSMemoryAllocator) Allocate(id string, size int64) (MemoryRegion, error) {
	var f *os.File
	var err error

	// Fallback to disk-backed shared memory.
	if err := os.MkdirAll(a.basePath, 0777); err != nil {
		return nil, fmt.Errorf("failed to create allocator base path: %w", err)
	}
	path := filepath.Join(a.basePath, id)
	f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err == nil {
		err = f.Truncate(size)
	}


	if err != nil {
		return nil, fmt.Errorf("failed to allocate memory backing: %w", err)
	}

	// Map the file descriptor into memory.
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	return &osMemoryRegion{data: data, file: f}, nil
}

func (a *OSMemoryAllocator) Attach(id string, path string) (MemoryRegion, error) {
	if path == "" {
		path = filepath.Join(a.basePath, id)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open handle %s: %w", path, err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat failed for handle %s: %w", path, err)
	}

	size := fi.Size()
	if size == 0 {
		f.Close()
		return nil, fmt.Errorf("handle %s is empty", path)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap failed for handle %s: %w", path, err)
	}

	return &osMemoryRegion{data: data, file: f}, nil
}

func (a *OSMemoryAllocator) Remove(id string) error {
	path := filepath.Join(a.basePath, id)
	return os.Remove(path)
}

func (a *OSMemoryAllocator) Cleanup() error {
	return os.RemoveAll(a.basePath)
}
