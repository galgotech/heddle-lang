//go:build linux

package data

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// createMemfd creates an anonymous file in RAM using memfd_create.
func createMemfd(name string, size int64) (*os.File, error) {
	fd, err := unix.MemfdCreate(name, 0)
	if err != nil {
		return nil, fmt.Errorf("memfd_create failed: %w", err)
	}

	if err := unix.Ftruncate(fd, size); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("ftruncate failed: %w", err)
	}

	return os.NewFile(uintptr(fd), name), nil
}

// supportsMemfd returns true if the current system supports memfd_create.
func supportsMemfd() bool {
	return true
}
