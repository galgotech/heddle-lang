package scaffold

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScaffoldService_Init(t *testing.T) {
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(originalWd)

	s := NewScaffoldService()
	err := s.Init("galgotech/test_project")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify structure
	if _, err := os.Stat("flows"); os.IsNotExist(err) {
		t.Error("expected flows directory to be created")
	}
	if _, err := os.Stat(filepath.Join("flows", "helloworld.he")); os.IsNotExist(err) {
		t.Error("expected helloworld.he to be created")
	}
	if _, err := os.Stat("heddle.toml"); os.IsNotExist(err) {
		t.Error("expected heddle.toml to be created")
	}
}
