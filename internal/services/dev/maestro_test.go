package dev

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMaestro_ScanWorkers(t *testing.T) {
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(originalWd)

	// Create valid structure
	validDir := filepath.Join("workers", "ns", "valid")
	os.MkdirAll(validDir, 0755)
	
	workerToml := `[plugin]
name = "valid"
namespace = "ns"
runtime = "python"
sdk_version = "0.1.0"
`
	os.WriteFile(filepath.Join(validDir, "worker.toml"), []byte(workerToml), 0644)

	m, err := NewMaestro()
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

	worker := m.Workers[validDir]
	if worker.Namespace != "ns" {
		t.Errorf("expected namespace ns, got %s", worker.Namespace)
	}
	if worker.PluginName != "valid" {
		t.Errorf("expected plugin name valid, got %s", worker.PluginName)
	}
	if worker.Language != "python" {
		t.Errorf("expected language python, got %s", worker.Language)
	}
}
