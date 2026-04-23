package main

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"
	"go.uber.org/zap"
)

const (
	lsName    = "heddle"
	lsVersion = "0.0.1"
)

var (
	state  *State
	logger *zap.Logger
)

func main() {
	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{"/tmp/heddle-lsp.log"}
	var err error
	logger, err = cfg.Build()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	state = NewState()

	ctx := context.Background()
	stream := jsonrpc2.NewStream(stdioRW{os.Stdin, os.Stdout})

	h := &lspHandler{
		client: nil, // Will be set after connection
		timers: make(map[protocol.DocumentURI]*time.Timer),
	}

	conn := jsonrpc2.NewConn(stream)
	h.client = protocol.ClientDispatcher(conn, logger)

	conn.Go(ctx, protocol.ServerHandler(h, jsonrpc2.MethodNotFoundHandler))

	logger.Info("Starting Heddle LSP server", zap.String("version", lsVersion))

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

	mu     sync.Mutex
	timers map[protocol.DocumentURI]*time.Timer
}

func (h *lspHandler) Initialize(ctx context.Context, params *protocol.InitializeParams) (*protocol.InitializeResult, error) {
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

func (h *lspHandler) Initialized(ctx context.Context, params *protocol.InitializedParams) error {
	return nil
}

func (h *lspHandler) Shutdown(ctx context.Context) error {
	return nil
}

func (h *lspHandler) Exit(ctx context.Context) error {
	return nil
}
