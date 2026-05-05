package ast

import (
	"testing"
)

func TestAcquireAndReleaseASTNode(t *testing.T) {
	node := AcquireASTNode(TypeLogicalStep)
	if node == nil {
		t.Fatalf("AcquireASTNode returned nil")
	}
	if node.Type() != TypeLogicalStep {
		t.Errorf("expected TypeLogicalStep, got %v", node.Type())
	}
	
	// Modify state
	lsNode := node.(*LogicalStepNode)
	lsNode.NextRef = 42

	// Release
	ReleaseASTNode(node)

	// Acquire again, ensure wiped
	node2 := AcquireASTNode(TypeLogicalStep)
	lsNode2 := node2.(*LogicalStepNode)
	if lsNode2.NextRef != 0 {
		t.Errorf("expected node to be wiped, got NextRef = %d", lsNode2.NextRef)
	}
}

func TestJoinNode(t *testing.T) {
	node := AcquireASTNode(TypeJoin)
	if node.Type() != TypeJoin {
		t.Errorf("expected TypeJoin")
	}
	node.(*JoinNode).LeftRef = 1
	ReleaseASTNode(node)
	
	node2 := AcquireASTNode(TypeJoin)
	if node2.(*JoinNode).LeftRef != 0 {
		t.Errorf("expected wipe")
	}
}

func TestFFICallNode(t *testing.T) {
	node := AcquireASTNode(TypeFFICall)
	if node.Type() != TypeFFICall {
		t.Errorf("expected TypeFFICall")
	}
	node.(*FFICallNode).NextRef = 99
	ReleaseASTNode(node)
	
	node2 := AcquireASTNode(TypeFFICall)
	if node2.(*FFICallNode).NextRef != 0 {
		t.Errorf("expected wipe")
	}
}

func BenchmarkASTNodePool(b *testing.B) {
	// Pre-warm the pools to avoid initial allocation counting
	n1 := AcquireASTNode(TypeLogicalStep)
	n2 := AcquireASTNode(TypeJoin)
	n3 := AcquireASTNode(TypeFFICall)
	ReleaseASTNode(n1)
	ReleaseASTNode(n2)
	ReleaseASTNode(n3)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Acquire a node
		node := AcquireASTNode(TypeLogicalStep)
		
		// Simulate setting some references
		if ls, ok := node.(*LogicalStepNode); ok {
			ls.NameRef = StringRef{Start: 0, End: 10}
			ls.InputRef = 1
			ls.NextRef = 2
		}
		
		// Release the node back to the pool
		ReleaseASTNode(node)
	}
}
