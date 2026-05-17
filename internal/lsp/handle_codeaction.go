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

// HandleCodeAction processes a "textDocument/codeAction" LSP request to retrieve a list of
// commands or edits that can be applied to the active document at the current user selection or range.
// It parses the file content, identifies active language constructs at the cursor using a Navigator,
// and returns actions such as "Organize Imports" or target-specific template generation (e.g., generating tests).
func HandleCodeAction(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map) error {
	// Unmarshal the incoming request parameters to extract the URI, cursor/selection range, and context.
	var params protocol.CodeActionParams
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	// Load the document content from the in-memory files cache.
	uri := params.TextDocument.URI
	text, ok := files.Load(uri)
	if !ok {
		return reply(ctx, nil, nil)
	}

	// Acquire a pooled AST context to perform zero-allocation parsing,
	// mitigating Garbage Collector pressure on high-frequency LSP requests.
	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	// Lex and parse the current source code to construct the AST.
	l := lexer.New(text.(string))
	p := parser.New(l, astCtx)
	prog := p.Parse()

	actions := []protocol.CodeAction{}

	// Construct and append the "Organize Imports" code action.
	// This action triggers a full-document formatting edit, which automatically organizes and formats imports.
	actions = append(actions, protocol.CodeAction{
		Title: "Organize Imports",
		Kind:  protocol.SourceOrganizeImports,
		Edit: &protocol.WorkspaceEdit{
			Changes: map[protocol.DocumentURI][]protocol.TextEdit{
				uri: organizeImports(ctx, uri, files),
			},
		},
	})

	// Query the AST at the specified line and character position (1-indexed in Heddle compiler constructs,
	// but 0-indexed in the LSP protocol) to determine if a workflow or step definition exists under the cursor.
	nav := compiler.NewNavigator(astCtx)
	symbolName, symbolType := nav.SymbolAt(prog, params.Range.Start.Line+1, params.Range.Start.Character+1)
	if symbolType == "workflow" || symbolType == "step" {
		// Provide an action to append a commented boilerplate test block at the end of the document.
		// A high line boundary (100,000) is specified to ensure the text is appended safely at the EOF.
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

// organizeImports reformats the entire document to organize import statements.
// It parses the current source file, formats the AST, and generates a single comprehensive
// TextEdit that replaces the active file content.
func organizeImports(ctx context.Context, uri protocol.DocumentURI, files *sync.Map) []protocol.TextEdit {
	// Retrieve current source text from the in-memory cache.
	text, ok := files.Load(uri)
	if !ok {
		return []protocol.TextEdit{}
	}

	// Acquire a pooled AST context to perform AST-related memory operations efficiently.
	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	// Lex, parse, and format the document AST.
	l := lexer.New(text.(string))
	p := parser.New(l, astCtx)
	prog := p.Parse()

	f := NewFormatter(astCtx)
	formatted := f.Format(prog)

	// Return a single full-document replacement edit targeting the entire file range (Line 0 to 100,000).
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
