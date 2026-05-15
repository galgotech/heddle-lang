package maestro

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

	workerToml := `[worker]
name = "valid"
namespace = "ns"
runtime = "go"
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

	executor := m.Workers[validDir]
	if executor.Name() != "ns/valid" {
		t.Errorf("expected name ns/valid, got %s", executor.Name())
	}
}

func TestGoExecutor_Start(t *testing.T) {
	tempDir := t.TempDir()

	// Create cmd/main.go
	cmdDir := filepath.Join(tempDir, "cmd")
	os.MkdirAll(cmdDir, 0755)

	mainGo := `package main
import "fmt"
import "os"
func main() {
	fmt.Println("Worker started")
	// Keep running until signaled
	fmt.Fprintf(os.Stderr, "Worker address: %s\n", os.Getenv("HEDDLE_WORKER_ADDRESS"))
	select {}
}
`
	os.WriteFile(filepath.Join(cmdDir, "main.go"), []byte(mainGo), 0644)

	// Create a minimal go.mod to satisfy go run .
	goMod := "module testworker\ngo 1.21\n"
	os.WriteFile(filepath.Join(tempDir, "go.mod"), []byte(goMod), 0644)

	executor := NewGoExecutor("ns", "testworker", tempDir)

	socketPath := filepath.Join(tempDir, "test.sock")
	err := executor.Start(t.Context(), socketPath)
	if err != nil {
		t.Fatalf("failed to start executor: %v", err)
	}

	// Check if process is running
	goExecutor := executor.(*GoExecutor)
	if goExecutor.Cmd == nil || goExecutor.Cmd.Process == nil {
		t.Fatal("expected process to be started")
	}

	// Cleanup
	err = executor.Stop()
	if err != nil {
		t.Errorf("failed to stop executor: %v", err)
	}
}
