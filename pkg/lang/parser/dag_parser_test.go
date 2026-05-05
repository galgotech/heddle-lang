package parser

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
)

func TestParseDAGAndRelease(t *testing.T) {
	l := lexer.New("step Foo:")
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	p := New(l, ctx)
	node := p.ParseDAG()

	if node == nil {
		t.Fatalf("ParseDAG returned nil")
	}
	if node.Type() != ast.TypeLogicalStep {
		t.Errorf("expected TypeLogicalStep, got %v", node.Type())
	}

	ReleaseDAG(node)

	// Release nil safe check
	ReleaseDAG(nil)
}
