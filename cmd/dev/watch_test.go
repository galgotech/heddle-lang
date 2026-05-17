package dev

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWatchCmd_ArgsValidation(t *testing.T) {
	// Test that passing more than 1 argument fails validation
	DevCmd.SetArgs([]string{"watch", "/path/to/project1", "/path/to/project2"})

	err := DevCmd.ExecuteContext(context.Background())
	assert.Error(t, err, "Expected an error when passing too many arguments")
}
