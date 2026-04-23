//go:build !linux

package data

import (
	"fmt"
	"os"
	"path/filepath"
)

// createMemfd falls back to a temporary file for non-Linux systems.
func createMemfd(name string, size int64) (*os.File, error) {
	// Fallback to /dev/shm if it exists, or os.TempDir
	shmPath := "/dev/shm"
	if _, err := os.Stat(shmPath); err != nil {
		shmPath = os.TempDir()
	}

	path := filepath.Join(shmPath, name)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to create fallback shm file: %w", err)
	}

	if err := f.Truncate(size); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("ftruncate failed: %w", err)
	}

	return f, nil
}

// supportsMemfd returns false for non-Linux systems.
func supportsMemfd() bool {
	return false
}
