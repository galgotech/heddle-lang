package lsp

import (
	"context"
	"encoding/json"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func handleDocumentSymbol(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map) error {
	var params protocol.DocumentSymbolParams
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

	nav := compiler.NewNavigator(astCtx)
	symbols := nav.DocumentSymbols(prog)

	lspSymbols := []protocol.DocumentSymbol{}
	for _, s := range symbols {
		kind := protocol.SymbolKindFunction
		switch s.Kind {
		case "resource":
			kind = protocol.SymbolKindVariable
		case "step":
			kind = protocol.SymbolKindFunction
		case "workflow":
			kind = protocol.SymbolKindClass
		case "handler":
			kind = protocol.SymbolKindMethod
		}

		lspSymbols = append(lspSymbols, protocol.DocumentSymbol{
			Name: s.Name,
			Kind: kind,
			Range: protocol.Range{
				Start: protocol.Position{Line: s.Range.Start.Line - 1, Character: s.Range.Start.Col - 1},
				End:   protocol.Position{Line: s.Range.End.Line - 1, Character: s.Range.End.Col - 1},
			},
			SelectionRange: protocol.Range{
				Start: protocol.Position{Line: s.Range.Start.Line - 1, Character: s.Range.Start.Col - 1},
				End:   protocol.Position{Line: s.Range.End.Line - 1, Character: s.Range.End.Col - 1},
			},
		})
	}

	return reply(ctx, lspSymbols, nil)
}
