package lsp

import (
	"context"
	"time"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"go.lsp.dev/protocol"
)

const (
	lsName    = "heddle-ls"
	lsVersion = "0.1.0"
)

type LSPHandler struct {
	protocol.Server
	Client protocol.Client
	state  *State
	timers map[protocol.DocumentURI]*time.Timer
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

func (h *LSPHandler) DidOpen(ctx context.Context, params *protocol.DidOpenTextDocumentParams) error {
	h.publishDiagnostics(ctx, params.TextDocument.URI, params.TextDocument.Text)
	return nil
}

func (h *LSPHandler) DidChange(ctx context.Context, params *protocol.DidChangeTextDocumentParams) error {
	if len(params.ContentChanges) == 0 {
		return nil
	}

	uri := params.TextDocument.URI
	text := params.ContentChanges[0].Text

	if timer, ok := h.timers[uri]; ok {
		timer.Stop()
	}

	h.timers[uri] = time.AfterFunc(200*time.Millisecond, func() {
		h.publishDiagnostics(ctx, uri, text)
	})

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
		content = "### Step: " + doc.Ctx.GetString(step.NameRef) + "\n"
		fn := doc.Ctx.FunctionRefNodes[step.FunctionRef]
		content += "**Implementation**: `" + doc.Ctx.GetString(fn.ModuleRef) + "." + doc.Ctx.GetString(fn.NameRef) + "`\n"
	case "call":
		call := doc.Ctx.CallNodes[ref]
		if call.IsPrql {
			content = "### PRQL Transformation\n```prql\n" + doc.Ctx.GetString(call.QueryRef) + "\n```"
		} else {
			name := doc.Ctx.GetString(call.NameRef)
			content = "### Call: " + name + "\n"
			stepRef := doc.Validator.Lookup(name)
			if stepRef != 0 {
				step := doc.Ctx.StepBindingNodes[stepRef]
				fn := doc.Ctx.FunctionRefNodes[step.FunctionRef]
				content += "**Step Implementation**: `" + doc.Ctx.GetString(fn.ModuleRef) + "." + doc.Ctx.GetString(fn.NameRef) + "`\n"
			}
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
		if i == 0 {
			continue
		}
		if h.inRange(r, pos) {
			return "call", ast.NodeRef(i)
		}
	}
	// Search in Steps
	for i, r := range doc.Ctx.StepRanges {
		if i == 0 {
			continue
		}
		if h.inRange(r, pos) {
			return "step", ast.NodeRef(i)
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
	if doc.Validator != nil {
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
	}

	h.Client.PublishDiagnostics(ctx, &protocol.PublishDiagnosticsParams{
		URI:         uri,
		Diagnostics: diagnostics,
	})
}

func (h *LSPHandler) Definition(ctx context.Context, params *protocol.DefinitionParams) ([]protocol.Location, error) {
	doc, ok := h.state.GetDocument(string(params.TextDocument.URI))
	if !ok {
		return nil, nil
	}

	pos := ast.Position{
		Line: uint32(params.Position.Line + 1),
		Col:  uint32(params.Position.Character + 1),
	}

	nodeType, ref := h.findNodeAt(doc, pos)
	if nodeType == "call" {
		call := doc.Ctx.CallNodes[ref]
		if !call.IsPrql {
			name := doc.Ctx.GetString(call.NameRef)
			stepRef := doc.Validator.Lookup(name)
			if stepRef != 0 {
				r := doc.Ctx.StepRanges[stepRef]
				return []protocol.Location{
					{
						URI: params.TextDocument.URI,
						Range: protocol.Range{
							Start: protocol.Position{Line: r.Start.Line - 1, Character: r.Start.Col - 1},
							End:   protocol.Position{Line: r.End.Line - 1, Character: r.End.Col - 1},
						},
					},
				}, nil
			}
		}
	}

	return nil, nil
}
