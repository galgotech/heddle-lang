package lsp_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/stretchr/testify/assert"

	"github.com/galgotech/heddle-lang/internal/lsp"
)

type dummyRW struct {
	io.Reader
	io.Writer
}

func (d dummyRW) Close() error {
	return nil
}

func TestServerStart(t *testing.T) {
	log := logger.NewNop()
	server := lsp.NewServer(log, "localhost:50051")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start(ctx, struct {
			io.Reader
			io.Writer
			io.Closer
		}{r1, w2, w2})
	}()

	// Wait a bit to ensure it has started, then cancel context
	time.Sleep(50 * time.Millisecond)
	cancel()
	w1.Close()
	r2.Close()

	err := <-errCh
	if err != nil {
		assert.Contains(t, err.Error(), context.Canceled.Error())
	}
}
