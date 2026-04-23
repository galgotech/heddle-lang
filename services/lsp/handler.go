package main

import (
	"context"
	"fmt"
	"time"

	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/ast"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

func (h *lspHandler) debouncedPublishDiagnostics(ctx context.Context, uri protocol.DocumentURI, text string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if timer, ok := h.timers[uri]; ok {
		timer.Stop()
	}

	h.timers[uri] = time.AfterFunc(200*time.Millisecond, func() {
		h.publishDiagnostics(context.Background(), uri, text)
	})
}

func (h *lspHandler) DidOpenTextDocument(ctx context.Context, params *protocol.DidOpenTextDocumentParams) error {
	logger.L().Info("Document opened", logger.String("uri", string(params.TextDocument.URI)))
	h.debouncedPublishDiagnostics(ctx, params.TextDocument.URI, params.TextDocument.Text)
	return nil
}

func (h *lspHandler) DidChangeTextDocument(ctx context.Context, params *protocol.DidChangeTextDocumentParams) error {
	if len(params.ContentChanges) > 0 {
		change := params.ContentChanges[0]
		h.debouncedPublishDiagnostics(ctx, params.TextDocument.URI, change.Text)
	}
	return nil
}

func (h *lspHandler) DidSaveTextDocument(ctx context.Context, params *protocol.DidSaveTextDocumentParams) error {
	return nil
}

func (h *lspHandler) Hover(ctx context.Context, params *protocol.HoverParams) (*protocol.Hover, error) {
	doc, ok := state.GetDocument(string(params.TextDocument.URI))
	if !ok {
		return nil, nil
	}

	node := ast.FindNodeAt(doc.Program, int(params.Position.Line+1), int(params.Position.Character+1))
	if node == nil {
		return nil, nil
	}

	var title string
	var description string

	switch n := node.(type) {
	case *ast.Identifier:
		title = "Identifier"
		description = fmt.Sprintf("Name: `%s`", n.Value)
	case *ast.StepCall:
		title = "Step Call"
		description = fmt.Sprintf("Executes step: `%s`", n.Name.Value)
	case *ast.SchemaRef:
		title = "Schema Reference"
		description = fmt.Sprintf("Type: `%s`", n.String())
	case *ast.FunctionRef:
		title = "Host Function"
		description = fmt.Sprintf("Implementation: `%s`", n.String())
	case *ast.ResourceBinding:
		title = "Resource Binding"
		description = fmt.Sprintf("Defines resource: `%s`", n.Name.Value)
	default:
		title = "Heddle Node"
		description = fmt.Sprintf("Type: `%T`", node)
	}

	content := fmt.Sprintf("### %s\n---\n%s", title, description)

	return &protocol.Hover{
		Contents: protocol.MarkupContent{
			Kind:  protocol.Markdown,
			Value: content,
		},
		Range: &protocol.Range{
			Start: params.Position,
			End:   params.Position, // Could be improved with actual node range
		},
	}, nil
}

func (h *lspHandler) publishDiagnostics(ctx context.Context, uri protocol.DocumentURI, text string) {
	logger.L().Info("Publishing diagnostics", logger.String("uri", string(uri)))

	_, parserErrors := state.UpdateDocument(string(uri), text)
	doc, _ := state.GetDocument(string(uri))

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

	h.client.PublishDiagnostics(ctx, &protocol.PublishDiagnosticsParams{
		URI:         uri,
		Diagnostics: diagnostics,
	})
}

func (h *lspHandler) Definition(ctx context.Context, params *protocol.DefinitionParams) ([]protocol.Location, error) {
	doc, ok := state.GetDocument(string(params.TextDocument.URI))
	if !ok {
		return nil, nil
	}

	node := ast.FindNodeAt(doc.Program, int(params.Position.Line+1), int(params.Position.Character+1))
	if node == nil {
		return nil, nil
	}

	var targetNode ast.Node

	switch n := node.(type) {
	case *ast.Identifier:
		targetNode = doc.Validator.Lookup(n.Value)
	case *ast.StepCall:
		targetNode = doc.Validator.Lookup(n.Name.Value)
	}

	if targetNode == nil {
		return nil, nil
	}

	r := ast.GetRange(targetNode)
	return []protocol.Location{
		{
			URI: params.TextDocument.URI,
			Range: protocol.Range{
				Start: protocol.Position{Line: uint32(r.Start.Line - 1), Character: uint32(r.Start.Column - 1)},
				End:   protocol.Position{Line: uint32(r.End.Line - 1), Character: uint32(r.End.Column - 1)},
			},
		},
	}, nil
}
