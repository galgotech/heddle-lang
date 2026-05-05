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
