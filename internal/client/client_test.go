package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewControlPlaneClient_ConnectionError(t *testing.T) {
	// Attempt to connect to a port that is closed
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client, err := NewControlPlaneClient(ctx, "127.0.0.1:59999")
	assert.Error(t, err)
	assert.Nil(t, client)
}
