package dev

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
	if _, err := os.Stat("workers"); os.IsNotExist(err) {
		t.Error("expected workers directory to be created")
	}
	if _, err := os.Stat(filepath.Join("flows", "helloworld.he")); os.IsNotExist(err) {
		t.Error("expected helloworld.he to be created")
	}
	if _, err := os.Stat("heddle.toml"); os.IsNotExist(err) {
		t.Error("expected heddle.toml to be created")
	}
}

func TestScaffoldService_WorkerAdd_Valid(t *testing.T) {
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(originalWd)

	s := NewScaffoldService()
	err := s.WorkerAdd("go", "galgotech/test_worker")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify scaffolding
	baseDir := filepath.Join("workers", "galgotech", "test_worker")

	if _, err := os.Stat(filepath.Join(baseDir, "worker.toml")); os.IsNotExist(err) {
		t.Error("expected worker.toml to be created")
	}

	if _, err := os.Stat(filepath.Join(baseDir, "main.go")); os.IsNotExist(err) {
		t.Error("expected main.go to be created")
	}

	if _, err := os.Stat(filepath.Join(baseDir, "go.mod")); os.IsNotExist(err) {
		t.Error("expected go.mod to be created")
	}

	if _, err := os.Stat(filepath.Join(baseDir, "config", "config.go")); os.IsNotExist(err) {
		t.Error("expected config/config.go to be created")
	}

	if _, err := os.Stat(filepath.Join(baseDir, "steps", "helloworld.go")); os.IsNotExist(err) {
		t.Error("expected steps/helloworld.go to be created")
	}

	if _, err := os.Stat(filepath.Join(baseDir, "resource", "resource.go")); os.IsNotExist(err) {
		t.Error("expected resource/resource.go to be created")
	}
}

func TestScaffoldService_WorkerAdd_InvalidNamespace(t *testing.T) {
	s := NewScaffoldService()
	err := s.WorkerAdd("go", "invalid-name-format")
	if err == nil {
		t.Fatal("expected error for invalid namespace format, got nil")
	}
}

func TestScaffoldService_WorkerValidate(t *testing.T) {
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(originalWd)

	// Create valid structure
	validDir := filepath.Join("workers", "ns", "valid")
	os.MkdirAll(validDir, 0755)
	os.WriteFile(filepath.Join(validDir, "worker.toml"), []byte(""), 0644)

	// Create invalid structure
	invalidDir := filepath.Join("workers", "invalid")
	os.MkdirAll(invalidDir, 0755)
	os.WriteFile(filepath.Join(invalidDir, "worker.toml"), []byte(""), 0644)

	s := NewScaffoldService()
	count, err := s.WorkerValidate()
	if err != nil {
		t.Fatalf("expected no error during validation, got %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 valid worker, got %d", count)
	}
}
