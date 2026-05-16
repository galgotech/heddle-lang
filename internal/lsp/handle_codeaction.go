package lsp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func HandleCodeAction(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map) error {
	var params protocol.CodeActionParams
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	uri := params.TextDocument.URI
	text, ok := files.Load(uri)
	if !ok {
		return reply(ctx, nil, nil)
	}

	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	l := lexer.New(text.(string))
	p := parser.New(l, astCtx)
	prog := p.Parse()

	actions := []protocol.CodeAction{}

	// Add "Organize Imports" action
	actions = append(actions, protocol.CodeAction{
		Title: "Organize Imports",
		Kind:  protocol.SourceOrganizeImports,
		Edit: &protocol.WorkspaceEdit{
			Changes: map[protocol.DocumentURI][]protocol.TextEdit{
				uri: organizeImports(ctx, uri, files),
			},
		},
	})

	// Check if cursor is on a workflow or step
	nav := compiler.NewNavigator(astCtx)
	symbolName, symbolType := nav.SymbolAt(prog, params.Range.Start.Line+1, params.Range.Start.Character+1)
	if symbolType == "workflow" || symbolType == "step" {
		actions = append(actions, protocol.CodeAction{
			Title: fmt.Sprintf("Add test for %s", symbolName),
			Kind:  protocol.RefactorRewrite,
			Edit: &protocol.WorkspaceEdit{
				Changes: map[protocol.DocumentURI][]protocol.TextEdit{
					uri: []protocol.TextEdit{
						{
							Range: protocol.Range{
								Start: protocol.Position{Line: 100000, Character: 0},
								End:   protocol.Position{Line: 100000, Character: 0},
							},
							NewText: fmt.Sprintf("\n\n// Test for %s\n// workflow test_%s {\n//   %s\n// }\n", symbolName, symbolName, symbolName),
						},
					},
				},
			},
		})
	}

	return reply(ctx, actions, nil)
}

func organizeImports(ctx context.Context, uri protocol.DocumentURI, files *sync.Map) []protocol.TextEdit {
	text, ok := files.Load(uri)
	if !ok {
		return []protocol.TextEdit{}
	}

	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	l := lexer.New(text.(string))
	p := parser.New(l, astCtx)
	prog := p.Parse()

	f := NewFormatter(astCtx)
	formatted := f.Format(prog)

	return []protocol.TextEdit{
		{
			Range: protocol.Range{
				Start: protocol.Position{Line: 0, Character: 0},
				End:   protocol.Position{Line: 100000, Character: 0},
			},
			NewText: formatted,
		},
	}
}
