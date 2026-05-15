package lsp

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

// Server represents the Heddle Language Server.
type Server struct {
	logger       *zap.Logger
	controlPlane *ControlPlaneLSPClient
	files        sync.Map // map[protocol.DocumentURI]string
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
			return reply(ctx, protocol.InitializeResult{
				Capabilities: protocol.ServerCapabilities{
					CompletionProvider: &protocol.CompletionOptions{
						TriggerCharacters: []string{".", ":", " ", ">"},
						ResolveProvider:   false,
					},
					TextDocumentSync: protocol.TextDocumentSyncOptions{
						OpenClose: true,
						Change:    protocol.TextDocumentSyncKindFull,
					},
					DocumentFormattingProvider: true,
					RenameProvider:             true,
					CodeActionProvider:         true,
					DefinitionProvider:         true,
					ReferencesProvider:         true,
					DocumentSymbolProvider:     true,
					SelectionRangeProvider:     true,
					WorkspaceSymbolProvider:    true,
					CodeLensProvider:           &protocol.CodeLensOptions{ResolveProvider: false},
				},
			}, nil)
		case protocol.MethodTextDocumentDidOpen:
			var params protocol.DidOpenTextDocumentParams
			if err := json.Unmarshal(req.Params(), &params); err == nil {
				s.files.Store(params.TextDocument.URI, params.TextDocument.Text)
				go s.validate(ctx, conn, params.TextDocument.URI, params.TextDocument.Text)
			}
			return nil
		case protocol.MethodTextDocumentDidChange:
			var params protocol.DidChangeTextDocumentParams
			if err := json.Unmarshal(req.Params(), &params); err == nil && len(params.ContentChanges) > 0 {
				s.files.Store(params.TextDocument.URI, params.ContentChanges[0].Text)
				go s.validate(ctx, conn, params.TextDocument.URI, params.ContentChanges[0].Text)
			}
			return nil
		case protocol.MethodTextDocumentCompletion:
			return s.handleCompletion(ctx, reply, req)
		case protocol.MethodTextDocumentFormatting:
			return s.handleFormatting(ctx, reply, req)
		case protocol.MethodTextDocumentRename:
			return s.handleRename(ctx, reply, req)
		case protocol.MethodTextDocumentCodeAction:
			return s.handleCodeAction(ctx, reply, req)
		case protocol.MethodTextDocumentDefinition:
			return s.handleDefinition(ctx, reply, req)
		case protocol.MethodTextDocumentReferences:
			return s.handleReferences(ctx, reply, req)
		case protocol.MethodTextDocumentDocumentSymbol:
			return s.handleDocumentSymbol(ctx, reply, req)
		case "textDocument/selectionRange":
			return s.handleSelectionRange(ctx, reply, req)
		case protocol.MethodWorkspaceSymbol:
			return s.handleWorkspaceSymbol(ctx, reply, req)
		case protocol.MethodTextDocumentCodeLens:
			return s.handleCodeLens(ctx, reply, req)
		}
		return jsonrpc2.MethodNotFoundHandler(ctx, reply, req)
	}
}

func (s *Server) validate(ctx context.Context, conn jsonrpc2.Conn, uri protocol.DocumentURI, text string) {
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
		diagnostics = append(diagnostics, getSemanticDiagnostics(ctx, s, prog, astCtx)...)
	}

	// Publish Diagnostics
	conn.Notify(ctx, protocol.MethodTextDocumentPublishDiagnostics, protocol.PublishDiagnosticsParams{
		URI:         uri,
		Diagnostics: diagnostics,
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

func (s *Server) getRegistry(ctx context.Context) (*models.RegistryInfo, error) {
	if s.controlPlane == nil || !s.controlPlane.IsConnected() {
		return nil, nil
	}
	return s.controlPlane.GetRegistry(ctx)
}
