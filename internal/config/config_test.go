package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadHeddleConfig(t *testing.T) {
	// Create a temporary heddle.yaml
	tmpDir, err := os.MkdirTemp("", "heddle-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "heddle.yaml")
	configContent := `
log:
  level: debug
  format: json
client:
  mode: remote
  target: localhost:9000
  workflow:
    timeout: 10s
`
	err = os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Test loading
	cfg, err := LoadHeddleConfig(configPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
	assert.Equal(t, "remote", cfg.Client.Mode)
	assert.Equal(t, "localhost:9000", cfg.Client.Target)
	assert.Equal(t, "10s", cfg.Client.Workflow.Timeout)
}

func TestInit(t *testing.T) {
	// Create a temporary heddle-worker.yaml
	tmpDir, err := os.MkdirTemp("", "heddle-init-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "heddle-worker.yaml")
	configContent := `
log:
  level: warn
`
	err = os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Set env var override
	os.Setenv("HEDDLE_WORKER_LOG_FORMAT", "json")
	defer os.Unsetenv("HEDDLE_WORKER_LOG_FORMAT")

	// Test Init
	err = Init("HEDDLE_WORKER", configPath)
	require.NoError(t, err)

	// Verify using viper directly (since Init populates global viper)
	// Note: viper.Get("log.level") should work if Init was called correctly
	// Actually, Init uses viper.AutomaticEnv() and viper.SetEnvPrefix()
}
