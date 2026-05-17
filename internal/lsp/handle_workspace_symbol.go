package lsp

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func handleWorkspaceSymbol(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map) error {
	var params protocol.WorkspaceSymbolParams
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	// For now, we'll search symbols in all open files
	allSymbols := []protocol.SymbolInformation{}
	files.Range(func(key, value any) bool {
		uri := key.(protocol.DocumentURI)
		text := value.(string)

		astCtx := ast.AcquireASTContext()
		defer ast.ReleaseASTContext(astCtx)

		l := lexer.New(text)
		p := parser.New(l, astCtx)
		prog := p.Parse()

		nav := NewNavigator(astCtx)
		symbols := nav.DocumentSymbols(prog)

		for _, sym := range symbols {
			// Basic fuzzy match (contains)
			if params.Query == "" || containsFold(sym.Name, params.Query) {
				kind := protocol.SymbolKindFunction
				allSymbols = append(allSymbols, protocol.SymbolInformation{
					Name: sym.Name,
					Kind: kind,
					Location: protocol.Location{
						URI: uri,
						Range: protocol.Range{
							Start: protocol.Position{Line: sym.Range.Start.Line - 1, Character: sym.Range.Start.Col - 1},
							End:   protocol.Position{Line: sym.Range.End.Line - 1, Character: sym.Range.End.Col - 1},
						},
					},
				})
			}
		}
		return true
	})

	return reply(ctx, allSymbols, nil)
}

func containsFold(s, substr string) bool {
	if substr == "" {
		return true
	}
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
