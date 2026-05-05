package data

import (
	"os"
	"testing"
)

func TestNewDataManager_Error(t *testing.T) {
	// Try to create DataManager in a path where we don't have permissions (or invalid)
	// /proc is usually a good candidate for "operation not permitted" on many systems if we try to Mkdir under it
	// Or just a path that is a file already.
	tmpFile, err := os.CreateTemp("", "heddle-dm-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	// Using the file path as base path for MkdirAll should fail
	_, err = NewDataManager(tmpFile.Name()+"/sub", 0)
	if err == nil {
		t.Error("expected error when creating DataManager with invalid path, but got nil")
	}
}
