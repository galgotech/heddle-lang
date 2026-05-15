package dap

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"

	"go.uber.org/zap"
)

// Server implements the Heddle Debug Adapter Protocol server.
type Server struct {
	logger *zap.Logger
	addr   string
	cpAddr string
}

// NewServer creates a new DAP server.
func NewServer(logger *zap.Logger, addr, cpAddr string) *Server {
	return &Server{
		logger: logger,
		addr:   addr,
		cpAddr: cpAddr,
	}
}

// Start begins listening for DAP connections.
func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	s.logger.Info("Heddle DAP Server listening", zap.String("addr", s.addr))

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				s.logger.Error("failed to accept connection", zap.Error(err))
				continue
			}
		}

		go s.handleConnection(ctx, conn)
	}
}

// StartStdio starts the DAP server on stdio.
func (s *Server) StartStdio(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	s.logger.Info("Heddle DAP Server starting on stdio")
	s.handleSession(ctx, stdin, stdout)
	return nil
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	s.handleSession(ctx, conn, conn)
}

func (s *Server) handleSession(ctx context.Context, r io.Reader, w io.Writer) {
	session := &Session{
		logger: s.logger,
		rw:     bufio.NewReadWriter(bufio.NewReader(r), bufio.NewWriter(w)),
		sendMu: sync.Mutex{},
		cpAddr: s.cpAddr,
	}
	session.serve(ctx)
}
