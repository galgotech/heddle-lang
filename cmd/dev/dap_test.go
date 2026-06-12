package dev

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDapCmd_Flags(t *testing.T) {
	// Verify that the control-plane-addr flag is defined on DapCmd
	flag := DapCmd.Flags().Lookup("control-plane-addr")
	assert.NotNil(t, flag, "Expected control-plane-addr flag to be defined on DapCmd")

	if flag != nil {
		assert.Equal(t, "localhost:50051", flag.DefValue, "Expected control-plane-addr default value to be localhost:50051")
	}
}
