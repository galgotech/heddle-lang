package lsp

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func HandleHover(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map, registryGetter func(context.Context) (*models.RegistryInfo, error)) error {
	var params protocol.HoverParams
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
	symbolName, _ := nav.SymbolAt(prog, params.Position.Line+1, params.Position.Character+1)
	if symbolName == "" {
		return reply(ctx, nil, nil)
	}

	// Check registry for step metadata
	registry, _ := registryGetter(ctx)
	if registry != nil {
		if step, ok := registry.Steps[symbolName]; ok {
			var markdown strings.Builder
			if step.Documentation != "" {
				markdown.WriteString(step.Documentation)
				markdown.WriteString("\n\n")
			}
			if step.SourceCode != "" {
				markdown.WriteString("```go\n")
				markdown.WriteString(step.SourceCode)
				markdown.WriteString("\n```")
			}

			if markdown.Len() > 0 {
				return reply(ctx, protocol.Hover{
					Contents: protocol.MarkupContent{
						Kind:  protocol.Markdown,
						Value: markdown.String(),
					},
				}, nil)
			}
		}
	}

	return reply(ctx, nil, nil)
}
