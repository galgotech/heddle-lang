package data

import (
	"os"
)

// MemoryRegion defines the contract for a mapped memory segment.
// It provides access to the raw byte slice and the underlying file descriptor
// handle required for zero-copy transfers via UDS.
type MemoryRegion interface {
	// Data returns the underlying byte slice mapped into the process address space.
	Data() []byte
	// File returns the underlying file handle (e.g., /dev/shm or memfd).
	File() *os.File
	// Unmap releases the memory mapping.
	Unmap() error
}

// MemoryAllocator abstracts the host-level memory management operations.
// This allows the DataManager to operate on "logical" memory blocks without
// being coupled to specific POSIX syscalls or filesystem paths.
type MemoryAllocator interface {
	// Allocate creates a new named memory region of the specified size.
	// It may use memfd_create or create a file in a shared memory directory.
	Allocate(id string, size int64) (MemoryRegion, error)

	// Attach opens and maps an existing memory region by its identifier or path.
	Attach(id string, path string) (MemoryRegion, error)

	// Remove deletes a specific memory region from the underlying storage.
	Remove(id string) error

	// Cleanup purges all managed memory regions.
	Cleanup() error
}
