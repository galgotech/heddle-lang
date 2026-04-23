package compiler

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCompileFiles(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "heddle-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	file1 := filepath.Join(tmpDir, "file1.he")
	file2 := filepath.Join(tmpDir, "file2.he")

	// Valid schema definition
	err = os.WriteFile(file1, []byte(`
schema User {
    id: int
    name: string
}
`), 0644)
	if err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	// Valid step and workflow definition
	err = os.WriteFile(file2, []byte(`
step read_users: void -> User = std.read {
    path: "users.csv"
}

workflow Main {
    read_users
}
`), 0644)
	if err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	c := New()
	ir, err := c.CompileFiles([]string{file1, file2})
	if err != nil {
		t.Fatalf("CompileFiles failed: %v", err)
	}

	if ir == nil {
		t.Fatal("Expected ProgramIR, got nil")
	}

	if len(ir.Workflows) != 1 {
		t.Errorf("Expected 1 workflow, got %d", len(ir.Workflows))
	}
}

func TestCompileFiles_Error(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "heddle-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	file1 := filepath.Join(tmpDir, "error.he")
	// Unclosed brace should cause a parser error
	err = os.WriteFile(file1, []byte(`
schema Error {
    id: int
`), 0644)
	if err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	c := New()
	_, err = c.CompileFiles([]string{file1})
	if err == nil {
		t.Fatal("Expected error due to unclosed brace, got nil")
	}
}
