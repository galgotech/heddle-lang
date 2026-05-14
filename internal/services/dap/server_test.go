package dap

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestDAPServer_Start(t *testing.T) {
	logger := zap.NewNop()
	addr := "localhost:0" // random port
	cpAddr := "localhost:50051"

	s := NewServer(logger, addr, cpAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Find a free port first to know where it will listen
	l, err := net.Listen("tcp", addr)
	assert.NoError(t, err)
	actualAddr := l.Addr().String()
	l.Close()

	s.addr = actualAddr

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Try to connect
	conn, err := net.Dial("tcp", actualAddr)
	assert.NoError(t, err)
	if err == nil {
		conn.Close()
	}

	cancel()
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("server didn't shut down in time")
	}
}
