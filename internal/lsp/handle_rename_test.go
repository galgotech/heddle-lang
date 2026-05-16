package lsp

import (
	"context"
	"encoding/json"
	"testing"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"
)

func TestHandleRename(t *testing.T) {
	s := NewServer(nil, "")
	ctx := context.Background()

	content := `import "std/io" io

workflow hello_world {
  test
  > test

  test
    | io.print
}`
	uri := protocol.DocumentURI("file:///test.he")
	s.files.Store(uri, content)

	// Rename the assignment 'test' to 'test2'
	// It is at line 5 (0-indexed line 4), column 5 (0-indexed col 4)
	params := protocol.RenameParams{
		TextDocumentPositionParams: protocol.TextDocumentPositionParams{
			TextDocument: protocol.TextDocumentIdentifier{URI: uri},
			Position:     protocol.Position{Line: 4, Character: 4},
		},
		NewName: "test2",
	}
	var result protocol.WorkspaceEdit
	reply := func(ctx context.Context, res interface{}, err error) error {
		if err != nil {
			return nil
		}
		b, _ := json.Marshal(res)
		json.Unmarshal(b, &result)
		return nil
	}

	// We use jsonrpc2.NewCall because Request interface is sealed (has unexported methods)
	// Or we can just call handleRename with a mocked replier and request.
	// Actually, let's see if we can just use jsonrpc2.Call which implements Request.
	req, _ := jsonrpc2.NewCall(jsonrpc2.NewNumberID(1), protocol.MethodTextDocumentRename, params)

	err := HandleRename(ctx, reply, req, &s.files)
	if err != nil {
		t.Fatal(err)
	}

	edits := result.Changes[uri]
	if len(edits) != 2 {
		t.Fatalf("expected 2 edits, got %d, result: %+v", len(edits), result)
	}

	// Verify edits: one for the assignment, one for the usage
	// Line 4 (assignment) and Line 6 (usage)
	foundLine4 := false
	foundLine6 := false
	for _, edit := range edits {
		if edit.Range.Start.Line == 4 {
			foundLine4 = true
		}
		if edit.Range.Start.Line == 6 {
			foundLine6 = true
		}
	}

	if !foundLine4 || !foundLine6 {
		t.Errorf("did not find expected edits at line 4 and 6. Edits: %+v", edits)
	}

	// Test Case 2: Rename the global step 'test' to 'test3'
	// Cursor on line 4 (0-indexed line 3), column 3 (0-indexed col 2)
	params = protocol.RenameParams{
		TextDocumentPositionParams: protocol.TextDocumentPositionParams{
			TextDocument: protocol.TextDocumentIdentifier{URI: uri},
			Position:     protocol.Position{Line: 3, Character: 2},
		},
		NewName: "test3",
	}
	result = protocol.WorkspaceEdit{} // Clear result
	req, _ = jsonrpc2.NewCall(jsonrpc2.NewNumberID(2), protocol.MethodTextDocumentRename, params)
	err = HandleRename(ctx, reply, req, &s.files)
	if err != nil {
		t.Fatal(err)
	}

	edits = result.Changes[uri]
	// Should only find 1 edit (at line 3) because others are shadowed or the assignment itself
	if len(edits) != 1 {
		t.Fatalf("expected 1 edit for global step rename, got %d, edits: %+v", len(edits), edits)
	}

	if edits[0].Range.Start.Line != 3 {
		t.Errorf("expected edit at line 3, got %+v", edits[0].Range)
	}
}
