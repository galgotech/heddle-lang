package lsp_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/services/lsp"
)

type dummyRW struct {
	io.Reader
	io.Writer
}

func (d dummyRW) Close() error {
	return nil
}

func TestServerStart(t *testing.T) {
	logger := zap.NewNop()
	server := lsp.NewServer(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rw := dummyRW{}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start(ctx, rw)
	}()

	// Wait a bit to ensure it has started, then cancel context
	time.Sleep(10 * time.Millisecond)
	cancel()

	err := <-errCh
	assert.NoError(t, err, "Server.Start should return nil when context is cancelled")
}
