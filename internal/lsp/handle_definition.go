package lsp

import (
	"context"
	"encoding/json"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func handleDefinition(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map, registryGetter func(context.Context) (*models.RegistryInfo, error)) error {
	var params protocol.DefinitionParams
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
	defRange := nav.DefinitionAt(prog, params.Position.Line+1, params.Position.Character+1)
	if defRange != nil {
		return reply(ctx, protocol.Location{
			URI: uri,
			Range: protocol.Range{
				Start: protocol.Position{Line: defRange.Start.Line - 1, Character: defRange.Start.Col - 1},
				End:   protocol.Position{Line: defRange.End.Line - 1, Character: defRange.End.Col - 1},
			},
		}, nil)
	}

	// If no local definition, check registry for external steps/resources
	symbolName, _ := nav.SymbolAt(prog, params.Position.Line+1, params.Position.Character+1)
	if symbolName != "" {
		registry, _ := registryGetter(ctx)
		if registry != nil {
			if step, ok := registry.Steps[symbolName]; ok && step.SourceFile != "" {
				return reply(ctx, protocol.Location{
					URI: protocol.DocumentURI("file://" + step.SourceFile),
					Range: protocol.Range{
						Start: protocol.Position{Line: uint32(step.SourceLine - 1), Character: 0},
						End:   protocol.Position{Line: uint32(step.SourceLine - 1), Character: 0},
					},
				}, nil)
			}
		}
	}

	return reply(ctx, nil, nil)
}
