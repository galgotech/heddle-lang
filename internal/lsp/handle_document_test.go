package lsp

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"
)

func TestHandleDidOpen(t *testing.T) {
	ctx := context.Background()
	files := &sync.Map{}
	uri := protocol.DocumentURI("file:///test.he")
	content := "workflow main {}"

	params := protocol.DidOpenTextDocumentParams{
		TextDocument: protocol.TextDocumentItem{
			URI:  uri,
			Text: content,
		},
	}

	req, err := jsonrpc2.NewCall(jsonrpc2.NewNumberID(1), protocol.MethodTextDocumentDidOpen, params)
	assert.NoError(t, err)

	// Set up channels to verify asynchronous validation invocation
	calledVal := make(chan bool, 1)
	validateFunc := func(cCtx context.Context, conn jsonrpc2.Conn, dURI protocol.DocumentURI, text string) {
		assert.Equal(t, uri, dURI)
		assert.Equal(t, content, text)
		calledVal <- true
	}

	err = handleDidOpen(ctx, req, nil, files, validateFunc)
	assert.NoError(t, err)

	// Verify the file was stored in the in-memory cache
	cached, ok := files.Load(uri)
	assert.True(t, ok)
	assert.Equal(t, content, cached.(string))

	// Verify validation function was called asynchronously
	select {
	case <-calledVal:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("validate function was not called asynchronously")
	}
}

func TestHandleDidChange(t *testing.T) {
	ctx := context.Background()
	files := &sync.Map{}
	uri := protocol.DocumentURI("file:///test.he")
	initialContent := "workflow old {}"
	newContent := "workflow new {}"

	// Pre-store initial content
	files.Store(uri, initialContent)

	params := protocol.DidChangeTextDocumentParams{
		TextDocument: protocol.VersionedTextDocumentIdentifier{
			TextDocumentIdentifier: protocol.TextDocumentIdentifier{
				URI: uri,
			},
		},
		ContentChanges: []protocol.TextDocumentContentChangeEvent{
			{
				Text: newContent,
			},
		},
	}

	req, err := jsonrpc2.NewCall(jsonrpc2.NewNumberID(1), protocol.MethodTextDocumentDidChange, params)
	assert.NoError(t, err)

	calledVal := make(chan bool, 1)
	validateFunc := func(cCtx context.Context, conn jsonrpc2.Conn, dURI protocol.DocumentURI, text string) {
		assert.Equal(t, uri, dURI)
		assert.Equal(t, newContent, text)
		calledVal <- true
	}

	err = handleDidChange(ctx, req, nil, files, validateFunc)
	assert.NoError(t, err)

	// Verify the file was updated in the in-memory cache
	cached, ok := files.Load(uri)
	assert.True(t, ok)
	assert.Equal(t, newContent, cached.(string))

	// Verify validation function was called asynchronously
	select {
	case <-calledVal:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("validate function was not called asynchronously")
	}
}
