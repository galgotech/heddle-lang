package lsp

import (
	"context"
	"sync"
	"time"

	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

const (
	lsName    = "heddle"
	lsVersion = "0.0.1"
)

type LSPHandler struct {
	protocol.Server
	Client protocol.Client

	mu     sync.Mutex
	timers map[protocol.DocumentURI]*time.Timer
	state  *State
}

func NewLSPHandler(state *State) *LSPHandler {
	return &LSPHandler{
		state:  state,
		timers: make(map[protocol.DocumentURI]*time.Timer),
	}
}

func (h *LSPHandler) Initialize(ctx context.Context, params *protocol.InitializeParams) (*protocol.InitializeResult, error) {
	return &protocol.InitializeResult{
		Capabilities: protocol.ServerCapabilities{
			TextDocumentSync: &protocol.TextDocumentSyncOptions{
				OpenClose: true,
				Change:    protocol.TextDocumentSyncKindFull,
			},
			HoverProvider:      true,
			DefinitionProvider: true,
			CompletionProvider: &protocol.CompletionOptions{
				TriggerCharacters: []string{"."},
			},
		},
		ServerInfo: &protocol.ServerInfo{
			Name:    lsName,
			Version: lsVersion,
		},
	}, nil
}

func (h *LSPHandler) Initialized(ctx context.Context, params *protocol.InitializedParams) error {
	return nil
}

func (h *LSPHandler) Shutdown(ctx context.Context) error {
	return nil
}

func (h *LSPHandler) Exit(ctx context.Context) error {
	return nil
}

func (h *LSPHandler) debouncedPublishDiagnostics(ctx context.Context, uri protocol.DocumentURI, text string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if timer, ok := h.timers[uri]; ok {
		timer.Stop()
	}

	h.timers[uri] = time.AfterFunc(200*time.Millisecond, func() {
		h.publishDiagnostics(context.Background(), uri, text)
	})
}

func (h *LSPHandler) DidOpenTextDocument(ctx context.Context, params *protocol.DidOpenTextDocumentParams) error {
	logger.L().Info("Document opened", logger.String("uri", string(params.TextDocument.URI)))
	h.debouncedPublishDiagnostics(ctx, params.TextDocument.URI, params.TextDocument.Text)
	return nil
}

func (h *LSPHandler) DidChangeTextDocument(ctx context.Context, params *protocol.DidChangeTextDocumentParams) error {
	if len(params.ContentChanges) > 0 {
		change := params.ContentChanges[0]
		h.debouncedPublishDiagnostics(ctx, params.TextDocument.URI, change.Text)
	}
	return nil
}

func (h *LSPHandler) DidSaveTextDocument(ctx context.Context, params *protocol.DidSaveTextDocumentParams) error {
	return nil
}

func (h *LSPHandler) Hover(ctx context.Context, params *protocol.HoverParams) (*protocol.Hover, error) {
	// TODO: Implement FindNodeAt for pointerless AST
	return nil, nil
}

func (h *LSPHandler) publishDiagnostics(ctx context.Context, uri protocol.DocumentURI, text string) {
	logger.L().Info("Publishing diagnostics", logger.String("uri", string(uri)))

	_, parserErrors := h.state.UpdateDocument(string(uri), text)
	doc, _ := h.state.GetDocument(string(uri))

	var diagnostics []protocol.Diagnostic

	// 1. Parser Errors
	for _, err := range parserErrors {
		diagnostics = append(diagnostics, protocol.Diagnostic{
			Range: protocol.Range{
				Start: protocol.Position{Line: uint32(err.Line - 1), Character: uint32(err.Column - 1)},
				End:   protocol.Position{Line: uint32(err.Line - 1), Character: uint32(err.Column + 5)}, // Arbitrary length
			},
			Severity: protocol.DiagnosticSeverityError,
			Source:   lsName,
			Message:  err.Message,
		})
	}

	// 2. Semantic Validation Errors
	if err := doc.Validator.Validate(); err != nil {
		diagnostics = append(diagnostics, protocol.Diagnostic{
			Range: protocol.Range{
				Start: protocol.Position{Line: 0, Character: 0},
				End:   protocol.Position{Line: 0, Character: 10},
			},
			Severity: protocol.DiagnosticSeverityWarning,
			Source:   lsName,
			Message:  err.Error(),
		})
	}

	h.Client.PublishDiagnostics(ctx, &protocol.PublishDiagnosticsParams{
		URI:         uri,
		Diagnostics: diagnostics,
	})
}

func (h *LSPHandler) Definition(ctx context.Context, params *protocol.DefinitionParams) ([]protocol.Location, error) {
	// TODO: Implement FindNodeAt and GetRange for pointerless AST
	return nil, nil
}
