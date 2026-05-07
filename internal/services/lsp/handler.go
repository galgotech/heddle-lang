package lsp

import (
	"context"
	"sync"
	"time"

	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
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
	doc, ok := h.state.GetDocument(string(params.TextDocument.URI))
	if !ok {
		return nil, nil
	}

	pos := ast.Position{
		Line: uint32(params.Position.Line + 1),
		Col:  uint32(params.Position.Character + 1),
	}

	nodeType, ref := h.findNodeAt(doc, pos)
	if nodeType == "" {
		return nil, nil
	}

	var content string
	switch nodeType {
	case "step":
		step := doc.Ctx.StepBindingNodes[ref]
		sig := doc.Ctx.StepSignatureNodes[step.SignatureRef]
		content = "### Step: " + doc.Ctx.GetString(step.NameRef) + "\n"
		content += "**Signature**: " + doc.Validator.TypeName(sig.InputRef) + " -> " + doc.Validator.TypeName(sig.OutputRef)
	case "schema":
		schema := doc.Ctx.SchemaNodes[ref]
		content = "### Schema: " + doc.Ctx.GetString(schema.NameRef)
	case "call":
		call := doc.Ctx.CallNodes[ref]
		name := doc.Ctx.GetString(call.NameRef)
		stepRef := doc.Validator.Lookup(name)
		if stepRef != 0 {
			step := doc.Ctx.StepBindingNodes[stepRef]
			sig := doc.Ctx.StepSignatureNodes[step.SignatureRef]
			content = "### Call: " + name + "\n"
			content += "**Signature**: " + doc.Validator.TypeName(sig.InputRef) + " -> " + doc.Validator.TypeName(sig.OutputRef)
		}
	}

	if content == "" {
		return nil, nil
	}

	return &protocol.Hover{
		Contents: protocol.MarkupContent{
			Kind:  protocol.Markdown,
			Value: content,
		},
	}, nil
}

func (h *LSPHandler) findNodeAt(doc *Document, pos ast.Position) (string, ast.NodeRef) {
	// Search in Calls (most common for hover)
	for i, r := range doc.Ctx.CallRanges {
		if h.inRange(r, pos) {
			return "call", ast.NodeRef(i)
		}
	}
	// Search in Steps
	for i, r := range doc.Ctx.StepRanges {
		if h.inRange(r, pos) {
			return "step", ast.NodeRef(i)
		}
	}
	// Search in Schemas
	for i, r := range doc.Ctx.SchemaRanges {
		if h.inRange(r, pos) {
			return "schema", ast.NodeRef(i)
		}
	}
	return "", 0
}

func (h *LSPHandler) inRange(r ast.Range, pos ast.Position) bool {
	if pos.Line < r.Start.Line || pos.Line > r.End.Line {
		return false
	}
	if pos.Line == r.Start.Line && pos.Col < r.Start.Col {
		return false
	}
	if pos.Line == r.End.Line && pos.Col > r.End.Col {
		return false
	}
	return true
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
	// For now, semantic errors don't have precise ranges in the validator,
	// so we highlight the first line if there's an error.
	if err := doc.Validator.Validate(); err != nil {
		diagnostics = append(diagnostics, protocol.Diagnostic{
			Range: protocol.Range{
				Start: protocol.Position{Line: 0, Character: 0},
				End:   protocol.Position{Line: 0, Character: 80},
			},
			Severity: protocol.DiagnosticSeverityError,
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
