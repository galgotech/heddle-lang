package lsp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func TestHandleCodeLens(t *testing.T) {
	s := NewServer(nil, "")
	ctx := context.Background()

	content := `import "std/io" io

step a = io.print
step b = io.print

handler my_handler {
  *
    | io.print
}

workflow my_workflow ? my_handler {
  a
    | b
}
`

	// Verify that the parser can successfully parse this content without error.
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

	params := protocol.CodeLensParams{
		TextDocument: protocol.TextDocumentIdentifier{URI: uri},
	}

	var result []protocol.CodeLens
	reply := func(ctx context.Context, res any, err error) error {
		if err != nil {
			return nil
		}
		b, _ := json.Marshal(res)
		json.Unmarshal(b, &result)
		return nil
	}

	req, _ := jsonrpc2.NewCall(jsonrpc2.NewNumberID(1), protocol.MethodTextDocumentCodeLens, params)

	err := handleCodeLens(ctx, reply, req, &s.files)
	if err != nil {
		t.Fatal(err)
	}

	// We expect 4 lenses: Run Workflow, Debug Workflow, Run Handler, Debug Handler.
	if len(result) != 4 {
		t.Fatalf("expected 4 code lenses, got %d: %+v", len(result), result)
	}

	expectedLenses := []struct {
		title   string
		command string
	}{
		{"▶ Run workflow", "heddle.runWorkflow"},
		{"🐞 Debug workflow", "heddle.debugWorkflow"},
		{"▶ Run handler", "heddle.runWorkflow"},
		{"🐞 Debug handler", "heddle.debugWorkflow"},
	}

	// Workflows are resolved first in HandleCodeLens, then Handlers.
	for i, exp := range expectedLenses {
		assert.Equal(t, exp.title, result[i].Command.Title)
		assert.Equal(t, exp.command, result[i].Command.Command)
		args := result[i].Command.Arguments
		assert.Len(t, args, 1)

		argMap, ok := args[0].(map[string]any)
		if !ok {
			argMapVal, _ := args[0].(map[string]interface{})
			argMap = map[string]any{}
			for k, v := range argMapVal {
				argMap[k] = v
			}
		}
		assert.Equal(t, string(uri), argMap["uri"])
		if i < 2 {
			assert.Equal(t, "my_workflow", argMap["workflow"])
		} else {
			assert.Equal(t, "my_handler", argMap["workflow"])
		}
	}
}
