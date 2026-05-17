package lsp

import (
	"context"
	"encoding/json"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"
)

// handleDidOpen processes the "textDocument/didOpen" LSP notification.
// It stores the opened document's initial content in the in-memory files cache and
// asynchronously triggers the document validation process to publish initial diagnostics.
func handleDidOpen(ctx context.Context, req jsonrpc2.Request, conn jsonrpc2.Conn, files *sync.Map, validate func(context.Context, jsonrpc2.Conn, protocol.DocumentURI, string)) error {
	var params protocol.DidOpenTextDocumentParams
	// Unmarshal the incoming request parameters to extract the file URI and initial document text.
	if err := json.Unmarshal(req.Params(), &params); err == nil {
		// Cache the document's initial state under its URI in the synchronized files map.
		files.Store(params.TextDocument.URI, params.TextDocument.Text)
		// Run validation asynchronously in a separate goroutine to avoid blocking the main connection event loop.
		go validate(ctx, conn, params.TextDocument.URI, params.TextDocument.Text)
	}
	return nil
}

// handleDidChange processes the "textDocument/didChange" LSP notification.
// It updates the cached document content with the new state (expecting full sync) and
// asynchronously re-runs validation to detect and publish syntax or semantic diagnostics.
func handleDidChange(ctx context.Context, req jsonrpc2.Request, conn jsonrpc2.Conn, files *sync.Map, validate func(context.Context, jsonrpc2.Conn, protocol.DocumentURI, string)) error {
	var params protocol.DidChangeTextDocumentParams
	// Parse the changed text document parameters and ensure there is at least one content update.
	if err := json.Unmarshal(req.Params(), &params); err == nil && len(params.ContentChanges) > 0 {
		// Cache the updated text under the document URI. Since the server operates under a full-document
		// synchronization model, ContentChanges[0].Text contains the complete current content.
		files.Store(params.TextDocument.URI, params.ContentChanges[0].Text)
		// Run validation asynchronously in a separate goroutine to avoid blocking the main connection event loop.
		go validate(ctx, conn, params.TextDocument.URI, params.ContentChanges[0].Text)
	}
	return nil
}
