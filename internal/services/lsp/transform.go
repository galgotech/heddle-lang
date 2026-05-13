package lsp

import (
	"context"
	"encoding/json"
	"fmt"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/formatter"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func (s *Server) handleFormatting(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
	var params protocol.DocumentFormattingParams
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

	if len(p.Errors()) > 0 {
		// Cannot format a document with syntax errors
		return reply(ctx, nil, nil)
	}

	f := formatter.New(astCtx)
	formatted := f.Format(prog)

	// Return a single text edit for the entire document
	// A more optimized formatter would return incremental edits
	return reply(ctx, []protocol.TextEdit{
		{
			Range: protocol.Range{
				Start: protocol.Position{Line: 0, Character: 0},
				End:   protocol.Position{Line: 100000, Character: 0}, // Approximation of end
			},
			NewText: formatted,
		},
	}, nil)
}

func (s *Server) handleRename(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
	var params protocol.RenameParams
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

	if len(p.Errors()) > 0 {
		return reply(ctx, nil, nil)
	}

	nav := compiler.NewNavigator(astCtx)
	symbolName, _ := nav.SymbolAt(prog, params.Position.Line+1, params.Position.Character+1)
	if symbolName == "" {
		return reply(ctx, nil, nil)
	}

	occurrences := nav.FindAllOccurrences(prog, symbolName)
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

func (s *Server) handleCodeAction(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
	var params protocol.CodeActionParams
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

	actions := []protocol.CodeAction{}

	// Add "Organize Imports" action
	actions = append(actions, protocol.CodeAction{
		Title: "Organize Imports",
		Kind:  protocol.SourceOrganizeImports,
		Edit: &protocol.WorkspaceEdit{
			Changes: map[protocol.DocumentURI][]protocol.TextEdit{
				uri: s.organizeImports(ctx, uri),
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
			// For simplicity, we'll just show the test code in a new file (not really possible via LSP easily without more complexity)
			// But we can append it to the current file or suggest a new file.
			// Let's use a Command that the client can handle or just an Edit.
			// We'll append a test block to the end of the file for now.
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

func (s *Server) organizeImports(ctx context.Context, uri protocol.DocumentURI) []protocol.TextEdit {
	text, ok := s.files.Load(uri)
	if !ok {
		return nil
	}

	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	l := lexer.New(text.(string))
	p := parser.New(l, astCtx)
	prog := p.Parse()

	if len(p.Errors()) > 0 {
		return nil
	}

	// We'll use the formatter logic but focused only on the import section
	// For simplicity, let's just format the whole file which already organizes things
	// because our Formatter iterates over definitions and regenerates code.

	f := formatter.New(astCtx)
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
