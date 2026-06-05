package logger

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoggerInit(t *testing.T) {
	err := Init(Config{
		Development: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, L())
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
	_ = Sync()
}

func TestFieldHelpers(t *testing.T) {
	f1 := String("key", "val")
	assert.NotNil(t, f1.zapField)

	f2 := Int("key", 123)
	assert.NotNil(t, f2.zapField)

	f3 := Int64("key64", 1234)
	assert.NotNil(t, f3.zapField)

	f4 := Float64("keyfloat", 12.34)
	assert.NotNil(t, f4.zapField)

	f5 := Error(errors.New("test error"))
	assert.NotNil(t, f5.zapField)

	f6 := Any("keyany", map[string]string{"a": "b"})
	assert.NotNil(t, f6.zapField)

	f7 := Strings("keystrings", []string{"a", "b"})
	assert.NotNil(t, f7.zapField)

	// Test that Info/Debug/Warn/Error/With work
	l := L().With(f1)
	l.Debug("debug", f2)
	l.Info("info", f3)
	l.Warn("warn", f4)
	l.Error("error", f5)
}
