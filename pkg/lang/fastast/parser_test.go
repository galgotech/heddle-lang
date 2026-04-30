package fastast

import (
	"testing"
)

func TestDummyParser(t *testing.T) {
	input := `dag pipeline1 { task extract "curl" task transform "jq" } dag pipeline2 { task load "psql" }`
	tokens := TokenizeDummy(input)

	ctx := AcquireASTContext()
	defer ReleaseASTContext(ctx)

	parser := NewDummyParser(tokens, ctx)
	program := parser.Parse()

	// Verify we got 2 DAGs
	dagCount := program.DAGRefsEnd - program.DAGRefsStart
	if dagCount != 2 {
		t.Fatalf("expected 2 DAGs, got %d", dagCount)
	}

	// Verify DAG 1
	dag1Ref := ctx.DAGRefs[program.DAGRefsStart]
	dag1 := ctx.DAGNodes[dag1Ref]

	dag1Name := ctx.GetString(dag1.NameRef)
	if dag1Name != "pipeline1" {
		t.Errorf("expected pipeline1, got %q", dag1Name)
	}

	// Verify Tasks in DAG 1
	dag1TaskCount := dag1.TaskRefsEnd - dag1.TaskRefsStart
	if dag1TaskCount != 2 {
		t.Fatalf("expected 2 tasks in pipeline1, got %d", dag1TaskCount)
	}

	task1Ref := ctx.TaskRefs[dag1.TaskRefsStart]
	task1 := ctx.TaskNodes[task1Ref]
	if ctx.GetString(task1.NameRef) != "extract" {
		t.Errorf("expected extract task, got %q", ctx.GetString(task1.NameRef))
	}
	if ctx.GetString(task1.CommandRef) != "curl" {
		t.Errorf("expected curl command, got %q", ctx.GetString(task1.CommandRef))
	}
}

func BenchmarkParserAllocs(b *testing.B) {
	input := `dag pipeline1 { task extract "curl" task transform "jq" } dag pipeline2 { task load "psql" }`
	tokens := TokenizeDummy(input)

	// Pre-warm the pool to ensure realistic measuring
	ctx1 := AcquireASTContext()
	ReleaseASTContext(ctx1)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ctx := AcquireASTContext()

		// Parse
		parser := NewDummyParser(tokens, ctx)
		_ = parser.Parse()

		ReleaseASTContext(ctx)
	}
}

func TestZeroAllocs(t *testing.T) {
	input := `dag pipeline1 { task extract "curl" task transform "jq" } dag pipeline2 { task load "psql" }`
	tokens := TokenizeDummy(input)

	// Warm up pool
	ctx1 := AcquireASTContext()
	ReleaseASTContext(ctx1)

	allocs := testing.AllocsPerRun(100, func() {
		ctx := AcquireASTContext()
		parser := NewDummyParser(tokens, ctx)
		_ = parser.Parse()
		ReleaseASTContext(ctx)
	})

	if allocs > 0 {
		t.Errorf("Expected 0 allocs per run, but got %v", allocs)
	}
}
