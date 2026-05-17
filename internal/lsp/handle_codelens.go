package lsp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

// handleCodeLens processes a textDocument/codeLens request. It parses the target document
// and returns executable actions (run/debug) for all defined workflows and handlers.
func handleCodeLens(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map) error {
	var params protocol.CodeLensParams
	// Unmarshal the incoming JSON-RPC request parameters into standard LSP CodeLensParams.
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	uri := params.TextDocument.URI
	// Look up the document text from the virtual file system sync.Map.
	val, ok := files.Load(uri)
	if !ok {
		return reply(ctx, []protocol.CodeLens{}, nil)
	}
	text := val.(string)

	// Acquire an AST context from the VictoriaMetrics-style sync.Pool to minimize GC allocation overhead.
	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	// Lex and parse the current source code to build a pointerless AST representation.
	l := lexer.New(text)
	p := parser.New(l, astCtx)
	prog := p.Parse()

	lenses := []protocol.CodeLens{}

	// Iterate over all workflow nodes identified in the parsed AST program to generate lenses.
	for i := prog.WorkflowRefsStart; i < prog.WorkflowRefsEnd; i++ {
		// Resolve workflow metadata and range coordinates from the AST context using stored references.
		workflowRef := astCtx.WorkflowRefs[i]
		workflow := astCtx.WorkflowNodes[workflowRef]
		workflowRange := astCtx.WorkflowRanges[workflowRef]
		name := astCtx.GetString(workflow.NameRef)

		lenses = append(lenses, createCodeLenses(uri, name, workflowRange, "workflow")...)
	}

	// Iterate over all handler nodes identified in the parsed AST program to generate lenses.
	for i := prog.HandlerRefsStart; i < prog.HandlerRefsEnd; i++ {
		// Resolve handler metadata and range coordinates from the AST context using stored references.
		handlerRef := astCtx.HandlerRefs[i]
		handler := astCtx.HandlerNodes[handlerRef]
		handlerRange := astCtx.HandlerRanges[handlerRef]
		name := astCtx.GetString(handler.NameRef)

		lenses = append(lenses, createCodeLenses(uri, name, handlerRange, "handler")...)
	}

	return reply(ctx, lenses, nil)
}

// createCodeLenses constructs both "Run" and "Debug" command-action CodeLenses for a given code block.
func createCodeLenses(uri protocol.DocumentURI, name string, r ast.Range, kind string) []protocol.CodeLens {
	// Convert the 1-indexed AST start line coordinates to LSP's 0-indexed protocol standard.
	line := uint32(r.Start.Line)
	if line > 0 {
		line--
	}

	protocolRange := protocol.Range{
		Start: protocol.Position{Line: line, Character: 0},
		End:   protocol.Position{Line: line, Character: 0},
	}

	// Return CodeLenses targeting the custom client commands for workflow execution and debugging.
	return []protocol.CodeLens{
		{
			Range: protocolRange,
			Command: &protocol.Command{
				Title:   fmt.Sprintf("▶ Run %s", kind),
				Command: "heddle.runWorkflow",
				Arguments: []interface{}{
					map[string]string{
						"uri":      string(uri),
						"workflow": name,
					},
				},
			},
		},
		{
			Range: protocolRange,
			Command: &protocol.Command{
				Title:   fmt.Sprintf("🐞 Debug %s", kind),
				Command: "heddle.debugWorkflow",
				Arguments: []interface{}{
					map[string]string{
						"uri":      string(uri),
						"workflow": name,
					},
				},
			},
		},
	}
}
