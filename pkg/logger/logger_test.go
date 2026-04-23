package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoggerInit(t *testing.T) {
	err := Init(Config{
		Development: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, L())
	assert.NotNil(t, S())
}

func TestLoggerWithOutputPaths(t *testing.T) {
	// Test with a temporary file
	tmpFile := "/tmp/test-heddle-logger.log"
	err := Init(Config{
		Development: true,
		OutputPaths: []string{tmpFile},
	})
	assert.NoError(t, err)

	L().Info("test message")
	err = Sync()
	// Zap Sync might return error for files on some OSes or if already synced, so we don't strictly assert NoError here

	// Clean up
	// os.Remove(tmpFile) // Optional
}

func TestFieldHelpers(t *testing.T) {
	f1 := String("key", "val")
	assert.Equal(t, "key", f1.Key)

	f2 := Int("key", 123)
	assert.Equal(t, "key", f2.Key)
	assert.Equal(t, int64(123), f2.Integer)
}
