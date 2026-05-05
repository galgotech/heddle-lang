package parser

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
)

func TestParser(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedErrs int
		check        func(*testing.T, *ast.ASTContext, ast.ProgramNode)
	}{
		{
			name: "Simple Workflow",
			input: `
workflow main {
  getData
    | process
  > result
}
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				wfCount := program.WorkflowRefsEnd - program.WorkflowRefsStart
				if wfCount != 1 {
					t.Fatalf("expected 1 workflow, got %d", wfCount)
				}
				wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
				wf := ctx.WorkflowNodes[wfRef]
				if ctx.GetString(wf.NameRef) != "main" {
					t.Errorf("expected workflow main, got %q", ctx.GetString(wf.NameRef))
				}
				stmtCount := wf.StatementRefsEnd - wf.StatementRefsStart
				if stmtCount != 1 {
					t.Fatalf("expected 1 statement in workflow, got %d", stmtCount)
				}
				psRef := ctx.StatementRefs[wf.StatementRefsStart]
				ps := ctx.PipelineStatementNodes[psRef]
				if ctx.GetString(ps.AssignmentRef) != "result" {
					t.Errorf("expected assignment to result, got %q", ctx.GetString(ps.AssignmentRef))
				}
			},
		},
		{
			name: "Schema and Import",
			input: `
import "std/http" http

schema User {
  name: string
  age: int
}
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				importCount := program.ImportRefsEnd - program.ImportRefsStart
				if importCount != 1 {
					t.Fatalf("expected 1 import, got %d", importCount)
				}
				schemaCount := program.SchemaRefsEnd - program.SchemaRefsStart
				if schemaCount != 1 {
					t.Fatalf("expected 1 schema, got %d", schemaCount)
				}
			},
		},
		{
			name: "Handler block",
			input: `
handler on_error {
  * error -> void = console.log
}
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				handlerCount := program.HandlerRefsEnd - program.HandlerRefsStart
				if handlerCount != 1 {
					t.Fatalf("expected 1 handler, got %d", handlerCount)
				}
				hRef := ctx.HandlerRefs[program.HandlerRefsStart]
				h := ctx.HandlerNodes[hRef]
				if ctx.GetString(h.NameRef) != "on_error" {
					t.Errorf("expected handler on_error, got %q", ctx.GetString(h.NameRef))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.input)
			ctx := ast.AcquireASTContext()
			defer ast.ReleaseASTContext(ctx)

			p := New(l, ctx)
			program := p.Parse()

			errs := p.Errors()
			if len(errs) != tt.expectedErrs {
				t.Fatalf("expected %d errors, got %d: %v", tt.expectedErrs, len(errs), errs)
			}

			if tt.check != nil {
				tt.check(t, ctx, program)
			}
		})
	}
}

func BenchmarkParser(b *testing.B) {
	input := `
import "std/http" http
schema User {
  name: string
  age: int
}

handdle on_error  {
  * error -> void = console.log
}

workflow main {
  getData
    | process ? on_error
  > result
}
`
	// Pre-warm the pool
	ctx1 := ast.AcquireASTContext()
	ast.ReleaseASTContext(ctx1)

	b.ReportAllocs()

	for b.Loop() {
		l := lexer.New(input)
		ctx := ast.AcquireASTContext()
		p := New(l, ctx)
		_ = p.Parse()
		ast.ReleaseASTContext(ctx)
	}
}
