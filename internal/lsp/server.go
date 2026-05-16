package lsp

import (
	"context"
	"io"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

// Server represents the Heddle Language Server.
type Server struct {
	logger       *zap.Logger
	controlPlane *ControlPlaneLSPClient
	files        sync.Map // map[protocol.DocumentURI]string
	trace        protocol.TraceValue
}

// Start begins processing language server requests.
func (s *Server) Start(ctx context.Context, rw io.ReadWriteCloser) error {
	s.logger.Info("Heddle Language Server starting on stdio", zap.String("control_plane", s.controlPlane.addr))

	stream := jsonrpc2.NewStream(rw)
	conn := jsonrpc2.NewConn(stream)

	// Pre-connect to control plane
	if err := s.controlPlane.Connect(ctx); err != nil {
		s.logger.Warn("Initial connection to control plane failed (will retry on demand)", zap.Error(err))
	}

	handler := s.handle(conn)
	conn.Go(ctx, handler)

	<-conn.Done()
	s.logger.Info("Heddle Language Server shutting down")
	s.controlPlane.Close()
	return nil
}

func (s *Server) handle(conn jsonrpc2.Conn) jsonrpc2.Handler {
	return func(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
		switch req.Method() {
		case protocol.MethodInitialize:
			return HandleInitialize(ctx, reply, req)
		case protocol.MethodSetTrace:
			trace, err := HandleSetTrace(ctx, req, s.logger)
			if err == nil {
				s.trace = trace
			}
			return err
		case protocol.MethodTextDocumentDidOpen:
			return HandleDidOpen(ctx, req, conn, &s.files, s.validate)
		case protocol.MethodTextDocumentDidChange:
			return HandleDidChange(ctx, req, conn, &s.files, s.validate)
		case protocol.MethodTextDocumentCompletion:
			return HandleCompletion(ctx, reply, req, &s.files, s.getRegistry, s.logger)
		case protocol.MethodTextDocumentFormatting:
			return HandleFormatting(ctx, reply, req, &s.files)
		case protocol.MethodTextDocumentRename:
			return HandleRename(ctx, reply, req, &s.files)
		case protocol.MethodTextDocumentCodeAction:
			return HandleCodeAction(ctx, reply, req, &s.files)
		case protocol.MethodTextDocumentDefinition:
			return HandleDefinition(ctx, reply, req, &s.files, s.getRegistry)
		case protocol.MethodTextDocumentHover:
			return HandleHover(ctx, reply, req, &s.files, s.getRegistry)
		case protocol.MethodTextDocumentReferences:
			return HandleReferences(ctx, reply, req, &s.files)
		case protocol.MethodTextDocumentDocumentSymbol:
			return HandleDocumentSymbol(ctx, reply, req, &s.files)
		case "textDocument/selectionRange":
			return HandleSelectionRange(ctx, reply, req, &s.files)
		case protocol.MethodWorkspaceSymbol:
			return HandleWorkspaceSymbol(ctx, reply, req, &s.files)
		case protocol.MethodTextDocumentCodeLens:
			return HandleCodeLens(ctx, reply, req, &s.files)
		}
		return jsonrpc2.MethodNotFoundHandler(ctx, reply, req)
	}
}

func (s *Server) validate(ctx context.Context, conn jsonrpc2.Conn, uri protocol.DocumentURI, text string) {
	s.logTrace(ctx, conn, "Validating document: "+string(uri), true)

	// Parse the document
	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	l := lexer.New(text)
	p := parser.New(l, astCtx)
	prog := p.Parse()

	diagnostics := []protocol.Diagnostic{}

	// Syntax Errors
	diagnostics = append(diagnostics, getSyntaxDiagnostics(p.Errors())...)

	// Semantic & Type Validation (only if syntax is ok)
	if len(p.Errors()) == 0 {
		diagnostics = append(diagnostics, getSemanticDiagnostics(ctx, prog, astCtx, s.getRegistry, s.logger)...)
	}

	// Publish Diagnostics
	conn.Notify(ctx, protocol.MethodTextDocumentPublishDiagnostics, protocol.PublishDiagnosticsParams{
		URI:         uri,
		Diagnostics: diagnostics,
	})
}

func (s *Server) getRegistry(ctx context.Context) (*models.RegistryInfo, error) {
	if s.controlPlane == nil || !s.controlPlane.IsConnected() {
		return nil, nil
	}
	return s.controlPlane.GetRegistry(ctx)
}

func (s *Server) logTrace(ctx context.Context, conn jsonrpc2.Conn, message string, verbose bool) {
	if s.trace == "off" || s.trace == "" {
		return
	}
	if verbose && s.trace != "verbose" {
		return
	}
	conn.Notify(ctx, "$/logTrace", protocol.LogTraceParams{
		Message: message,
	})
}

// NewServer creates a new instance of the LSP Server.
func NewServer(logger *zap.Logger, cpAddr string) *Server {
	return &Server{
		logger:       logger,
		controlPlane: NewControlPlaneLSPClient(cpAddr),
		files:        sync.Map{},
	}
}
