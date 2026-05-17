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

func handleSelectionRange(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map) error {
	var params protocol.SelectionRangeParams
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

	nav := NewNavigator(astCtx)

	results := []protocol.SelectionRange{}
	for _, pos := range params.Positions {
		ranges := nav.SelectionRanges(prog, pos.Line+1, pos.Character+1)
		if len(ranges) == 0 {
			results = append(results, protocol.SelectionRange{
				Range: protocol.Range{Start: pos, End: pos},
			})
			continue
		}

		var last *protocol.SelectionRange
		for i := 0; i < len(ranges); i++ {
			r := ranges[i]
			current := &protocol.SelectionRange{
				Range: protocol.Range{
					Start: protocol.Position{Line: r.Start.Line - 1, Character: r.Start.Col - 1},
					End:   protocol.Position{Line: r.End.Line - 1, Character: r.End.Col - 1},
				},
				Parent: last,
			}
			last = current
		}
		results = append(results, *last)
	}

	return reply(ctx, results, nil)
}
