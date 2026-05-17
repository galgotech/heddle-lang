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

	err := handleRename(ctx, reply, req, &s.files)
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
	err = handleRename(ctx, reply, req, &s.files)
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

	// Test Case 3: Precise step name rename in step definition block
	contentStep := `import "std/io" io

step test = <broker=kafka> io.test {
  config: "teste"
}

workflow hello_world {
  test
}`
	uriStep := protocol.DocumentURI("file:///step.he")
	s.files.Store(uriStep, contentStep)

	// Rename the step 'test' to 'test3'
	// Cursor is at "test" on line 3 (0-indexed line 2), column 6 (0-indexed col 5)
	params = protocol.RenameParams{
		TextDocumentPositionParams: protocol.TextDocumentPositionParams{
			TextDocument: protocol.TextDocumentIdentifier{URI: uriStep},
			Position:     protocol.Position{Line: 2, Character: 5},
		},
		NewName: "test3",
	}
	result = protocol.WorkspaceEdit{} // Clear result
	req, _ = jsonrpc2.NewCall(jsonrpc2.NewNumberID(3), protocol.MethodTextDocumentRename, params)
	err = handleRename(ctx, reply, req, &s.files)
	if err != nil {
		t.Fatal(err)
	}

	edits = result.Changes[uriStep]
	if len(edits) != 2 {
		t.Fatalf("expected 2 edits (one for definition name, one for usage in workflow), got %d, result: %+v", len(edits), result)
	}

	// Verify that the definition edit only replaces the identifier "test", not the whole block!
	// "test" is on line 2 (0-indexed), col 5 to 9.
	var foundDefEdit bool
	var foundUsageEdit bool
	for _, edit := range edits {
		if edit.Range.Start.Line == 2 {
			foundDefEdit = true
			if edit.Range.Start.Character != 5 || edit.Range.End.Character != 9 {
				t.Errorf("expected definition name range to be exactly line 2 col 5 to 9, got start %d end %d", edit.Range.Start.Character, edit.Range.End.Character)
			}
		}
		if edit.Range.Start.Line == 7 {
			foundUsageEdit = true
			if edit.Range.Start.Character != 2 || edit.Range.End.Character != 6 {
				t.Errorf("expected workflow usage range to be exactly line 7 col 2 to 6, got start %d end %d", edit.Range.Start.Character, edit.Range.End.Character)
			}
		}
	}

	if !foundDefEdit {
		t.Errorf("did not find definition edit on line 2")
	}
	if !foundUsageEdit {
		t.Errorf("did not find usage edit on line 7")
	}

	// Test Case 4: Rename import alias
	contentImport := `import "std/io" io

workflow hello_world {
  []
    | io.print
}`
	uriImport := protocol.DocumentURI("file:///import.he")
	s.files.Store(uriImport, contentImport)

	// Rename the import alias 'io' to 'io2'
	// Cursor is on "io" in import statement on line 1 (0-indexed line 0), column 17 (0-indexed col 16)
	params = protocol.RenameParams{
		TextDocumentPositionParams: protocol.TextDocumentPositionParams{
			TextDocument: protocol.TextDocumentIdentifier{URI: uriImport},
			Position:     protocol.Position{Line: 0, Character: 16},
		},
		NewName: "io2",
	}
	result = protocol.WorkspaceEdit{} // Clear result
	req, _ = jsonrpc2.NewCall(jsonrpc2.NewNumberID(4), protocol.MethodTextDocumentRename, params)
	err = handleRename(ctx, reply, req, &s.files)
	if err != nil {
		t.Fatal(err)
	}

	edits = result.Changes[uriImport]
	if len(edits) != 2 {
		t.Fatalf("expected 2 edits (one for import alias, one for qualified call prefix), got %d, result: %+v", len(edits), result)
	}

	var foundImportAliasEdit bool
	var foundQualifiedCallEdit bool
	for _, edit := range edits {
		if edit.Range.Start.Line == 0 {
			foundImportAliasEdit = true
			if edit.Range.Start.Character != 16 || edit.Range.End.Character != 18 {
				t.Errorf("expected import alias range to be exactly line 0 col 16 to 18, got start %d end %d", edit.Range.Start.Character, edit.Range.End.Character)
			}
		}
		if edit.Range.Start.Line == 4 {
			foundQualifiedCallEdit = true
			if edit.Range.Start.Character != 6 || edit.Range.End.Character != 8 {
				t.Errorf("expected qualified call prefix range to be exactly line 4 col 6 to 8, got start %d end %d", edit.Range.Start.Character, edit.Range.End.Character)
			}
		}
	}

	if !foundImportAliasEdit {
		t.Errorf("did not find import alias edit on line 0")
	}
	if !foundQualifiedCallEdit {
		t.Errorf("did not find qualified call prefix edit on line 4")
	}
}
