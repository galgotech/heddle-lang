package main

import (
	"context"
	"log"

	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/ast"
)

func (h *lspHandler) DidOpenTextDocument(ctx context.Context, params *protocol.DidOpenTextDocumentParams) error {
	log.Printf("Document opened: %s", params.TextDocument.URI)
	h.publishDiagnostics(ctx, params.TextDocument.URI, params.TextDocument.Text)
	return nil
}

func (h *lspHandler) DidChangeTextDocument(ctx context.Context, params *protocol.DidChangeTextDocumentParams) error {
	if len(params.ContentChanges) > 0 {
		change := params.ContentChanges[0]
		h.publishDiagnostics(ctx, params.TextDocument.URI, change.Text)
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

	var content string
	switch n := node.(type) {
	case *ast.Identifier:
		content = "Identifier: " + n.Value
	case *ast.StepCall:
		content = "Step Call: " + n.Name.Value
	case *ast.SchemaRef:
		content = "Schema Reference: " + n.String()
	case *ast.FunctionRef:
		content = "Host Function: " + n.String()
	default:
		content = "Node: " + node.String()
	}

	return &protocol.Hover{
		Contents: protocol.MarkupContent{
			Kind:  protocol.Markdown,
			Value: content,
		},
	}, nil
}

func (h *lspHandler) publishDiagnostics(ctx context.Context, uri protocol.DocumentURI, text string) {
	log.Printf("Publishing diagnostics for %s", uri)

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
