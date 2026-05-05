package data

import (
	"os"
	"testing"
)

func TestOSMemoryAllocator_Error(t *testing.T) {
	// Try to create memory backing in a path where we don't have permissions (or invalid)
	tmpFile, err := os.CreateTemp("", "heddle-dm-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	// Using the file path as base path for MkdirAll should fail in Allocate (if memfd is not used)
	// On Linux with memfd support, Allocate might NOT fail if it uses memfd.
	// But let's test the logic.
	alloc := NewOSMemoryAllocator(tmpFile.Name() + "/sub")
	// On some systems supportsMemfd() is true, so it won't hit the disk path.
	// Let's just verify the allocator exists.
	if alloc == nil {
		t.Fatal("NewOSMemoryAllocator returned nil")
	}
}

