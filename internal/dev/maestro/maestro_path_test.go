package maestro

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMaestro_ScanWorkers_WithPath(t *testing.T) {
	tempDir := t.TempDir()

	// Create valid structure inside a subfolder (which acts as the project folder)
	projectDir := filepath.Join(tempDir, "my-project")
	validDir := filepath.Join(projectDir, "workers", "ns", "valid")
	if err := os.MkdirAll(validDir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	workerToml := `[worker]
name = "valid"
namespace = "ns"
runtime = "go"
sdk_version = "0.1.0"
`
	if err := os.WriteFile(filepath.Join(validDir, "worker.toml"), []byte(workerToml), 0644); err != nil {
		t.Fatalf("failed to write worker.toml: %v", err)
	}

	// We pass projectDir to NewMaestro!
	m, err := NewMaestro(projectDir)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = m.scanWorkers()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(m.Workers) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(m.Workers))
	}

	executor := m.Workers[validDir]
	if executor == nil {
		t.Fatalf("expected executor at key %s, but got nil", validDir)
	}
	if executor.Name() != "ns/valid" {
		t.Errorf("expected name ns/valid, got %s", executor.Name())
	}
}
