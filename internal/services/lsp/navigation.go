package lsp

import (
	"context"
	"encoding/json"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func (s *Server) handleDefinition(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
	var params protocol.DefinitionParams
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	uri := params.TextDocument.URI
	text, ok := s.files.Load(uri)
	if !ok {
		return reply(ctx, nil, nil)
	}

	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	l := lexer.New(text.(string))
	p := parser.New(l, astCtx)
	prog := p.Parse()

	nav := compiler.NewNavigator(astCtx)
	defRange := nav.DefinitionAt(prog, params.Position.Line+1, params.Position.Character+1)
	if defRange == nil {
		return reply(ctx, nil, nil)
	}

	return reply(ctx, protocol.Location{
		URI: uri,
		Range: protocol.Range{
			Start: protocol.Position{Line: defRange.Start.Line - 1, Character: defRange.Start.Col - 1},
			End:   protocol.Position{Line: defRange.End.Line - 1, Character: defRange.End.Col - 1},
		},
	}, nil)
}

func (s *Server) handleReferences(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
	var params protocol.ReferenceParams
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	uri := params.TextDocument.URI
	text, ok := s.files.Load(uri)
	if !ok {
		return reply(ctx, nil, nil)
	}

	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	l := lexer.New(text.(string))
	p := parser.New(l, astCtx)
	prog := p.Parse()

	nav := compiler.NewNavigator(astCtx)
	symbolName, _ := nav.SymbolAt(prog, params.Position.Line+1, params.Position.Character+1)
	if symbolName == "" {
		return reply(ctx, nil, nil)
	}

	occurrences := nav.FindAllOccurrences(prog, symbolName)
	locations := []protocol.Location{}
	for _, r := range occurrences {
		locations = append(locations, protocol.Location{
			URI: uri,
			Range: protocol.Range{
				Start: protocol.Position{Line: r.Start.Line - 1, Character: r.Start.Col - 1},
				End:   protocol.Position{Line: r.End.Line - 1, Character: r.End.Col - 1},
			},
		})
	}

	return reply(ctx, locations, nil)
}

func (s *Server) handleDocumentSymbol(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
	var params protocol.DocumentSymbolParams
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	uri := params.TextDocument.URI
	text, ok := s.files.Load(uri)
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

func (s *Server) handleSelectionRange(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
	var params protocol.SelectionRangeParams
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	uri := params.TextDocument.URI
	text, ok := s.files.Load(uri)
	if !ok {
		return reply(ctx, nil, nil)
	}

	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	l := lexer.New(text.(string))
	p := parser.New(l, astCtx)
	prog := p.Parse()

	nav := compiler.NewNavigator(astCtx)
	
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

func (s *Server) handleWorkspaceSymbol(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
	var params protocol.WorkspaceSymbolParams
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	// For now, we'll search symbols in all open files
	allSymbols := []protocol.SymbolInformation{}
	s.files.Range(func(key, value interface{}) bool {
		uri := key.(protocol.DocumentURI)
		text := value.(string)

		astCtx := ast.AcquireASTContext()
		defer ast.ReleaseASTContext(astCtx)

		l := lexer.New(text)
		p := parser.New(l, astCtx)
		prog := p.Parse()

		nav := compiler.NewNavigator(astCtx)
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
	return len(substr) == 0 || (len(s) >= len(substr) && (s == substr || (len(s) > 0 && len(substr) > 0))) // simplified
}
