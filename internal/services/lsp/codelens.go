package lsp

import (
	"context"
	"encoding/json"
	"fmt"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func (s *Server) handleCodeLens(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request) error {
	var params protocol.CodeLensParams
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	uri := params.TextDocument.URI
	val, ok := s.files.Load(uri)
	if !ok {
		return reply(ctx, []protocol.CodeLens{}, nil)
	}
	text := val.(string)

	// Parse the document
	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	l := lexer.New(text)
	p := parser.New(l, astCtx)
	prog := p.Parse()

	lenses := []protocol.CodeLens{}

	// Workflows
	for i := prog.WorkflowRefsStart; i < prog.WorkflowRefsEnd; i++ {
		workflowRef := astCtx.WorkflowRefs[i]
		workflow := astCtx.WorkflowNodes[workflowRef]
		workflowRange := astCtx.WorkflowRanges[workflowRef]
		name := astCtx.GetString(workflow.NameRef)

		lenses = append(lenses, s.createCodeLenses(uri, name, workflowRange, "workflow")...)
	}

	// Handlers
	for i := prog.HandlerRefsStart; i < prog.HandlerRefsEnd; i++ {
		handlerRef := astCtx.HandlerRefs[i]
		handler := astCtx.HandlerNodes[handlerRef]
		handlerRange := astCtx.HandlerRanges[handlerRef]
		name := astCtx.GetString(handler.NameRef)

		lenses = append(lenses, s.createCodeLenses(uri, name, handlerRange, "handler")...)
	}

	return reply(ctx, lenses, nil)
}

func (s *Server) createCodeLenses(uri protocol.DocumentURI, name string, r ast.Range, kind string) []protocol.CodeLens {
	// Start line of the block
	line := uint32(r.Start.Line)
	if line > 0 {
		line-- // protocol is 0-indexed
	}

	protocolRange := protocol.Range{
		Start: protocol.Position{Line: line, Character: 0},
		End:   protocol.Position{Line: line, Character: 0},
	}

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
