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

// Server coordinates the lifecycle, request routing, and real-time validation for Heddle Language Server sessions.
// It caches active workspace documents, maps syntax/semantic diagnostics, and maintains connectivity
// to the Heddle Control Plane to dynamically fetch and cross-reference registered steps and connectors.
type Server struct {
	// logger provides structured, level-based diagnostic reporting.
	logger *zap.Logger
	// controlPlane manages the gRPC/LSP client connection to the Heddle Control Plane.
	controlPlane *ControlPlaneLSPClient
	// files caches active in-memory document buffers, mapping protocol.DocumentURI to source strings.
	files sync.Map
	// trace determines the client's preferred request/response logging verbosity.
	trace protocol.TraceValue
}

// Start initiates processing of client requests over a bidirectional stream wrapper.
// It establishes a baseline connection to the Heddle Control Plane, spawns the main
// message-handling loop in a background goroutine, and blocks until the stream terminates.
func (s *Server) Start(ctx context.Context, rw io.ReadWriteCloser) error {
	s.logger.Info("Heddle Language Server starting on stdio", zap.String("control_plane", s.controlPlane.addr))

	// Initialize the JSON-RPC 2.0 stream and high-performance connection wrappers.
	stream := jsonrpc2.NewStream(rw)
	conn := jsonrpc2.NewConn(stream)

	// Pre-establish a connection to the Heddle Control Plane to populate step schemas.
	// In the event of network/startup failures, fallback gracefully to on-demand reconnection retries.
	if err := s.controlPlane.Connect(ctx); err != nil {
		s.logger.Warn("Initial connection to control plane failed (will retry on demand)", zap.Error(err))
	}

	// Register the unified request multiplexer and process incoming messages asynchronously.
	handler := s.handle(conn)
	conn.Go(ctx, handler)

	// Block execution until the JSON-RPC connection closes, then tear down resources.
	<-conn.Done()
	s.logger.Info("Heddle Language Server shutting down")
	s.controlPlane.Close()
	return nil
}

// handle returns a JSON-RPC handler function that acts as the primary LSP router.
// It intercepts and forwards client requests to dedicated feature-specific handlers.
func (s *Server) handle(conn jsonrpc2.Conn) jsonrpc2.Handler {
	return func(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
		switch req.Method() {
		// MethodInitialize handles the client-server handshake (negotiates editor capabilities).
		case protocol.MethodInitialize:
			return handleInitialize(ctx, reply, req)
		// MethodSetTrace configures the client's request/response logging verbosity (e.g. VS Code Output panel settings).
		case protocol.MethodSetTrace:
			trace, err := HandleSetTrace(ctx, req, s.logger)
			if err == nil {
				s.trace = trace
			}
			return err
		// MethodTextDocumentDidOpen handles buffer caching when a .he file is first opened in VS Code.
		case protocol.MethodTextDocumentDidOpen:
			return handleDidOpen(ctx, req, conn, &s.files, s.validate)
		// MethodTextDocumentDidChange drives active document parsing and validation as the user types in VS Code.
		case protocol.MethodTextDocumentDidChange:
			return handleDidChange(ctx, req, conn, &s.files, s.validate)
		// MethodTextDocumentCompletion triggers VS Code IntelliSense auto-complete popups (resources, steps, etc.).
		case protocol.MethodTextDocumentCompletion:
			return handleCompletion(ctx, reply, req, &s.files, s.getRegistry, s.logger)
		// MethodTextDocumentFormatting powers the VS Code Format Document command and format-on-save logic.
		case protocol.MethodTextDocumentFormatting:
			return handleFormatting(ctx, reply, req, &s.files)
		// MethodTextDocumentRename handles the VS Code Rename Symbol (F2) cross-file refactoring engine.
		case protocol.MethodTextDocumentRename:
			return handleRename(ctx, reply, req, &s.files)
		// MethodTextDocumentCodeAction populates the VS Code Quick Fix lightbulb menu with local file resolutions.
		case protocol.MethodTextDocumentCodeAction:
			return handleCodeAction(ctx, reply, req, &s.files)
		// MethodTextDocumentDefinition drives Go to Definition (F12 / Ctrl+Click) to locate workflows or custom connectors.
		case protocol.MethodTextDocumentDefinition:
			return handleDefinition(ctx, reply, req, &s.files, s.getRegistry)
		// MethodTextDocumentHover renders context-aware documentation tooltips when hovering over workflows/steps in VS Code.
		case protocol.MethodTextDocumentHover:
			return handleHover(ctx, reply, req, &s.files, s.getRegistry)
		// MethodTextDocumentReferences powers Find All References (Shift+F12) to trace variable or connector usages.
		case protocol.MethodTextDocumentReferences:
			return handleReferences(ctx, reply, req, &s.files)
		// MethodTextDocumentDocumentSymbol populates the VS Code Outline Panel and Breadcrumbs navigation.
		case protocol.MethodTextDocumentDocumentSymbol:
			return handleDocumentSymbol(ctx, reply, req, &s.files)
		// selectionRange supports Smart Select (Shift+Alt+Right/Left) to expand or shrink selections syntactically.
		case "textDocument/selectionRange":
			return handleSelectionRange(ctx, reply, req, &s.files)
		// MethodWorkspaceSymbol powers Go to Symbol in Workspace (Ctrl+T) searching across all workspace directories.
		case protocol.MethodWorkspaceSymbol:
			return handleWorkspaceSymbol(ctx, reply, req, &s.files)
		// MethodTextDocumentCodeLens renders clickable metadata labels/actions (e.g. "Run Workflow") directly in the VS Code editor buffer.
		case protocol.MethodTextDocumentCodeLens:
			return handleCodeLens(ctx, reply, req, &s.files)
		}
		// Return standard jsonrpc2.MethodNotFound error for unsupported request signatures.
		return jsonrpc2.MethodNotFoundHandler(ctx, reply, req)
	}
}

// validate parses the current document source, generates diagnostics, and publishes results back to the client.
// It leverages pooled, pointerless AST contexts to eliminate runtime garbage collection (GC) allocation overhead.
func (s *Server) validate(ctx context.Context, conn jsonrpc2.Conn, uri protocol.DocumentURI, text string) {
	// Log validation execution trace details back to the client if tracking is enabled.
	s.logTrace(ctx, conn, "Validating document: "+string(uri), true)

	// Retrieve a pointerless AST context from the pre-allocated context pool to avoid heap escapes.
	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	// Lexically analyze and parse the DSL document to build the Abstract Syntax Tree representation.
	l := lexer.New(text)
	p := parser.New(l, astCtx)
	prog := p.Parse()

	diagnostics := []protocol.Diagnostic{}

	// Map parser-level syntax errors into formal LSP Diagnostic objects.
	diagnostics = append(diagnostics, getSyntaxDiagnostics(p.Errors())...)

	// Evaluate semantic rules, verify custom step definitions, and enforce type contracts only if syntax is valid.
	if len(p.Errors()) == 0 {
		diagnostics = append(diagnostics, getSemanticDiagnostics(ctx, prog, astCtx, s.getRegistry, s.logger)...)
	}

	// Dispatch the diagnostics list to the client editor to update IDE warning/error underlines.
	conn.Notify(ctx, protocol.MethodTextDocumentPublishDiagnostics, protocol.PublishDiagnosticsParams{
		URI:         uri,
		Diagnostics: diagnostics,
	})
}

// getRegistry queries the Heddle Control Plane to retrieve schema configurations for all registered steps.
// It returns a nil registry if the underlying client is uninitialized or not currently connected.
func (s *Server) getRegistry(ctx context.Context) (*models.RegistryInfo, error) {
	if s.controlPlane == nil || !s.controlPlane.IsConnected() {
		return nil, nil
	}
	return s.controlPlane.GetRegistry(ctx)
}

// logTrace forwards telemetry/diagnostic logs to the LSP client based on client-specified trace filtering.
func (s *Server) logTrace(ctx context.Context, conn jsonrpc2.Conn, message string, verbose bool) {
	// Silently return if telemetry tracing has been disabled by the client.
	if s.trace == "off" || s.trace == "" {
		return
	}
	// Suppress verbose messages unless the client explicitly requested a verbose trace level.
	if verbose && s.trace != "verbose" {
		return
	}
	// Notify client connection using the standard protocol $/logTrace notification.
	conn.Notify(ctx, "$/logTrace", protocol.LogTraceParams{
		Message: message,
	})
}

// NewServer initializes a new Server instance configured with a structured logger and a Control Plane client.
func NewServer(logger *zap.Logger, cpAddr string) *Server {
	return &Server{
		logger:       logger,
		controlPlane: NewControlPlaneLSPClient(cpAddr),
		files:        sync.Map{},
	}
}
