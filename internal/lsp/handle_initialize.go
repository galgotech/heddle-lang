package lsp

import (
	"context"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"
)

func handleInitialize(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
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
			HoverProvider:              true,
			CodeLensProvider:           &protocol.CodeLensOptions{ResolveProvider: false},
		},
	}, nil)
}
