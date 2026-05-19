package local

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestStartLocalServices_Ready(t *testing.T) {
	// This test is tricky because it starts real services on fixed paths.
	// We'll skip it if it's already running or if we don't want to mess with /tmp.
	if os.Getenv("CI") == "" {
		t.Skip("Skipping local service test in non-CI environment to avoid messing with /tmp")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Clean up before test
	os.Remove("/tmp/heddle-cp.sock")
	os.Remove("/tmp/heddle-worker.sock")

	err := StartLocalServices(ctx)
	if err != nil {
		t.Fatalf("StartLocalServices failed: %v", err)
	}
}

func TestStartLocalServices_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := StartLocalServices(ctx)
	if err == nil {
		t.Error("Expected error for cancelled context, got nil")
	}
}

func TestStartCmd_ForegroundTimeout(t *testing.T) {
	if os.Getenv("CI") == "" {
		t.Skip("Skipping integration test in non-CI environment")
	}

	// Clean up before test
	os.Remove("/tmp/heddle-cp.sock")
	os.Remove("/tmp/heddle-worker.sock")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	LocalCmd.SetArgs([]string{"start"})

	startTime := time.Now()
	err := LocalCmd.ExecuteContext(ctx)
	duration := time.Since(startTime)

	if err != nil {
		t.Logf("startCmd exited with expected context timeout/error: %v", err)
	}

	if duration < 150*time.Millisecond {
		t.Errorf("Expected startCmd to run for at least 150ms (foreground wait), but it completed in %v", duration)
	}
}

