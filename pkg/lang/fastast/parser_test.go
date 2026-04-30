package fastast

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func TestParser(t *testing.T) {
	input := `
import "std/http" http

schema User {
    name: string
    age: int
}

workflow main {
    getData | process?onErr > result
}
`
	l := lexer.New(input)
	ctx := AcquireASTContext()
	defer ReleaseASTContext(ctx)

	p := NewParser(l, ctx)
	program := p.Parse()

	// Verify Imports
	importCount := program.ImportRefsEnd - program.ImportRefsStart
	if importCount != 1 {
		t.Fatalf("expected 1 import, got %d", importCount)
	}

	// Verify Schemas
	schemaCount := program.SchemaRefsEnd - program.SchemaRefsStart
	if schemaCount != 1 {
		t.Fatalf("expected 1 schema, got %d", schemaCount)
	}

	// Verify Workflows
	wfCount := program.WorkflowRefsEnd - program.WorkflowRefsStart
	if wfCount != 1 {
		t.Fatalf("expected 1 workflow, got %d", wfCount)
	}

	wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
	wf := ctx.WorkflowNodes[wfRef]
	if ctx.GetString(wf.NameRef) != "main" {
		t.Errorf("expected workflow main, got %q", ctx.GetString(wf.NameRef))
	}

	// Verify Statements in workflow
	stmtCount := wf.StatementRefsEnd - wf.StatementRefsStart
	if stmtCount != 1 {
		t.Fatalf("expected 1 statement in workflow, got %d", stmtCount)
	}

	psRef := ctx.StatementRefs[wf.StatementRefsStart]
	ps := ctx.PipelineStatementNodes[psRef]
	if ctx.GetString(ps.AssignmentRef) != "result" {
		t.Errorf("expected assignment to result, got %q", ctx.GetString(ps.AssignmentRef))
	}
}

func TestZeroAllocs(t *testing.T) {
	input := `workflow main {
  getData
    | process
}`

	// Warm up pool
	ctx1 := AcquireASTContext()
	ReleaseASTContext(ctx1)

	allocs := testing.AllocsPerRun(100, func() {
		l := lexer.New(input)
		ctx := AcquireASTContext()
		p := NewParser(l, ctx)
		_ = p.Parse()
		ReleaseASTContext(ctx)
	})

	// The lexer itself might allocate (it's not pointerless yet),
	// but the AST construction should be zero-alloc.
	// For now, let's just see what we get.
	t.Logf("Allocs per run: %v", allocs)
}

func BenchmarkFastParser(b *testing.B) {
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
}`
	// Pre-warm the pool
	ctx1 := AcquireASTContext()
	ReleaseASTContext(ctx1)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		l := lexer.New(input)
		ctx := AcquireASTContext()
		p := NewParser(l, ctx)
		_ = p.Parse()
		ReleaseASTContext(ctx)
	}
}

func BenchmarkStandardParser(b *testing.B) {
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
}`
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		l := lexer.New(input)
		p := parser.New(l)
		_ = p.Parse()
	}
}
