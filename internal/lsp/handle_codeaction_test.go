package lsp

import (
	"context"
	"encoding/json"
	"testing"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func TestHandleCodeAction(t *testing.T) {
	s := NewServer(nil, "")
	ctx := context.Background()

	content := `import "std/io" io

step test = io.print

workflow hello_world {
  test
}
`
	// Verify that the parser can successfully parse this content with no errors
	astCtxVerify := ast.AcquireASTContext()
	lVerify := lexer.New(content)
	pVerify := parser.New(lVerify, astCtxVerify)
	_ = pVerify.Parse()
	if len(pVerify.Errors()) > 0 {
		t.Fatalf("DSL content has parser errors: %+v", pVerify.Errors())
	}
	ast.ReleaseASTContext(astCtxVerify)

	uri := protocol.DocumentURI("file:///test.he")
	s.files.Store(uri, content)

	// Test Case 1: Cursor not on a workflow or step (e.g. Line 0, character 0 on the import statement)
	params := protocol.CodeActionParams{
		TextDocument: protocol.TextDocumentIdentifier{URI: uri},
		Range: protocol.Range{
			Start: protocol.Position{Line: 0, Character: 0},
			End:   protocol.Position{Line: 0, Character: 0},
		},
	}

	var result []protocol.CodeAction
	reply := func(ctx context.Context, res any, err error) error {
		if err != nil {
			return nil
		}
		b, _ := json.Marshal(res)
		json.Unmarshal(b, &result)
		return nil
	}

	req, _ := jsonrpc2.NewCall(jsonrpc2.NewNumberID(1), protocol.MethodTextDocumentCodeAction, params)

	err := handleCodeAction(ctx, reply, req, &s.files)
	if err != nil {
		t.Fatal(err)
	}

	// Should only have the "Organize Imports" action
	if len(result) != 1 {
		t.Fatalf("expected 1 code action, got %d: %+v", len(result), result)
	}
	if result[0].Title != "Organize Imports" {
		t.Errorf("expected 'Organize Imports', got %q", result[0].Title)
	}

	// Test Case 2: Cursor on the step "test" (Line 2, character 5 is on the 't' of 'test')
	params.Range.Start = protocol.Position{Line: 2, Character: 5}
	params.Range.End = protocol.Position{Line: 2, Character: 5}
	result = nil

	req, _ = jsonrpc2.NewCall(jsonrpc2.NewNumberID(2), protocol.MethodTextDocumentCodeAction, params)
	err = handleCodeAction(ctx, reply, req, &s.files)
	if err != nil {
		t.Fatal(err)
	}

	// Should have "Organize Imports" and "Add test for test"
	if len(result) != 2 {
		t.Fatalf("expected 2 code actions, got %d: %+v", len(result), result)
	}

	foundTest := false
	for _, act := range result {
		if act.Title == "Add test for test" {
			foundTest = true
			if act.Kind != protocol.RefactorRewrite {
				t.Errorf("expected RefactorRewrite kind, got %q", act.Kind)
			}
			edits := act.Edit.Changes[uri]
			if len(edits) != 1 {
				t.Fatalf("expected 1 edit, got %d", len(edits))
			}
			if edits[0].Range.Start.Line != 100000 {
				t.Errorf("expected start line 100000, got %d", edits[0].Range.Start.Line)
			}
		}
	}
	if !foundTest {
		t.Error("expected 'Add test for test' code action")
	}

	// Test Case 3: Cursor on the workflow "hello_world" (Line 4, character 9 is on the 'h' of 'hello_world')
	params.Range.Start = protocol.Position{Line: 4, Character: 9}
	params.Range.End = protocol.Position{Line: 4, Character: 9}
	result = nil

	req, _ = jsonrpc2.NewCall(jsonrpc2.NewNumberID(3), protocol.MethodTextDocumentCodeAction, params)
	err = handleCodeAction(ctx, reply, req, &s.files)
	if err != nil {
		t.Fatal(err)
	}

	// Should have "Organize Imports" and "Add test for hello_world"
	if len(result) != 2 {
		t.Fatalf("expected 2 code actions, got %d: %+v", len(result), result)
	}

	foundWorkflowTest := false
	for _, act := range result {
		if act.Title == "Add test for hello_world" {
			foundWorkflowTest = true
			if act.Kind != protocol.RefactorRewrite {
				t.Errorf("expected RefactorRewrite kind, got %q", act.Kind)
			}
			edits := act.Edit.Changes[uri]
			if len(edits) != 1 {
				t.Fatalf("expected 1 edit, got %d", len(edits))
			}
			if edits[0].Range.Start.Line != 100000 {
				t.Errorf("expected start line 100000, got %d", edits[0].Range.Start.Line)
			}
		}
	}
	if !foundWorkflowTest {
		t.Error("expected 'Add test for hello_world' code action")
	}
}
