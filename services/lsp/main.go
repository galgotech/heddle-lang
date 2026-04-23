package main

import (
	"context"
	"io"
	"log"
	"os"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"
)

const (
	lsName    = "heddle"
	lsVersion = "0.0.1"
)

var (
	state *State
)

func main() {
	state = NewState()

	ctx := context.Background()
	stream := jsonrpc2.NewStream(stdioRW{os.Stdin, os.Stdout})

	h := &lspHandler{
		client: nil, // Will be set after connection
	}

	conn := jsonrpc2.NewConn(stream)
	h.client = protocol.ClientDispatcher(conn, nil)

	conn.Go(ctx, protocol.ServerHandler(h, nil))

	log.Printf("Starting Heddle LSP server (go.lsp.dev) on stdin/stdout...")

	<-conn.Done()
}

type stdioRW struct {
	io.Reader
	io.Writer
}

func (stdioRW) Close() error {
	return nil
}

type lspHandler struct {
	protocol.Server
	client protocol.Client
}

func (h *lspHandler) Initialize(ctx context.Context, params *protocol.InitializeParams) (*protocol.InitializeResult, error) {
	return &protocol.InitializeResult{
		Capabilities: protocol.ServerCapabilities{
			TextDocumentSync: &protocol.TextDocumentSyncOptions{
				OpenClose: true,
				Change:    protocol.TextDocumentSyncKindFull,
			},
			HoverProvider: true,
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

func (h *lspHandler) Initialized(ctx context.Context, params *protocol.InitializedParams) error {
	return nil
}

func (h *lspHandler) Shutdown(ctx context.Context) error {
	return nil
}

func (h *lspHandler) Exit(ctx context.Context) error {
	return nil
}
