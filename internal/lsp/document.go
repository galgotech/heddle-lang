package lsp

import (
	"context"
	"encoding/json"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"
)

func HandleDidOpen(ctx context.Context, req jsonrpc2.Request, conn jsonrpc2.Conn, files *sync.Map, validate func(context.Context, jsonrpc2.Conn, protocol.DocumentURI, string)) error {
	var params protocol.DidOpenTextDocumentParams
	if err := json.Unmarshal(req.Params(), &params); err == nil {
		files.Store(params.TextDocument.URI, params.TextDocument.Text)
		go validate(ctx, conn, params.TextDocument.URI, params.TextDocument.Text)
	}
	return nil
}

func HandleDidChange(ctx context.Context, req jsonrpc2.Request, conn jsonrpc2.Conn, files *sync.Map, validate func(context.Context, jsonrpc2.Conn, protocol.DocumentURI, string)) error {
	var params protocol.DidChangeTextDocumentParams
	if err := json.Unmarshal(req.Params(), &params); err == nil && len(params.ContentChanges) > 0 {
		files.Store(params.TextDocument.URI, params.ContentChanges[0].Text)
		go validate(ctx, conn, params.TextDocument.URI, params.ContentChanges[0].Text)
	}
	return nil
}
