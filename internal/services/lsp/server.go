package lsp

import (
	"context"
	"io"

	"go.uber.org/zap"
)

// Server represents the Heddle Language Server.
type Server struct {
	logger *zap.Logger
}

// NewServer creates a new instance of the LSP Server.
func NewServer(logger *zap.Logger) *Server {
	return &Server{
		logger: logger,
	}
}

// Start begins processing language server requests.
func (s *Server) Start(ctx context.Context, rw io.ReadWriteCloser) error {
	s.logger.Info("Heddle Language Server starting on stdio")

	// TODO: Implement actual LSP protocol handling
	// This usually involves reading JSON-RPC messages from rw and dispatching
	// them to handlers. We keep it open to block the command so the client
	// doesn't immediately disconnect.
	<-ctx.Done()

	s.logger.Info("Heddle Language Server shutting down")
	return nil
}
