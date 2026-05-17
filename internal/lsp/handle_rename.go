package lsp

import (
	"context"
	"encoding/json"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func handleRename(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map) error {
	var params protocol.RenameParams
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

	if len(p.Errors()) > 0 {
		return reply(ctx, nil, nil)
	}

	nav := NewNavigator(astCtx)
	symbolName, symbolType := nav.SymbolAt(prog, params.Position.Line+1, params.Position.Character+1)
	if symbolName == "" {
		return reply(ctx, nil, nil)
	}

	occurrences := nav.FindAllOccurrences(prog, symbolName, symbolType, params.Position.Line+1, params.Position.Character+1)
	edits := []protocol.TextEdit{}
	for _, r := range occurrences {
		edits = append(edits, protocol.TextEdit{
			Range: protocol.Range{
				Start: protocol.Position{Line: r.Start.Line - 1, Character: r.Start.Col - 1},
				End:   protocol.Position{Line: r.End.Line - 1, Character: r.End.Col - 1},
			},
			NewText: params.NewName,
		})
	}

	return reply(ctx, protocol.WorkspaceEdit{
		Changes: map[protocol.DocumentURI][]protocol.TextEdit{
			uri: edits,
		},
	}, nil)
}
