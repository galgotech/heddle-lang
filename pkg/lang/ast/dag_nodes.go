package ast

import "sync"

// NodeType identifies the type of ASTNode.
type NodeType uint8

const (
	TypeLogicalStep NodeType = iota
	TypeJoin
	TypeFFICall
)

// ASTNode represents a unified interface for all DAG nodes.
// By using this interface, the interpreter can seamlessly iterate through the DAG.
type ASTNode interface {
	Type() NodeType
	Evaluate() error // Lifecycle method for DAG evaluation
	Reset()          // Clears the internal state before returning to pool
}

// LogicalStepNode represents a logical step in the DAG.
// It is a "pointerless struct" to hide the AST trees from the GC tracing phase.
type LogicalStepNode struct {
	NameRef  StringRef
	InputRef uint32
	NextRef  uint32
}

func (n *LogicalStepNode) Type() NodeType  { return TypeLogicalStep }
func (n *LogicalStepNode) Evaluate() error { return nil }
func (n *LogicalStepNode) Reset()          { *n = LogicalStepNode{} }

// JoinNode represents a join operation in the DAG.
// Must be pointerless.
type JoinNode struct {
	LeftRef  uint32
	RightRef uint32
	NextRef  uint32
}

func (n *JoinNode) Type() NodeType  { return TypeJoin }
func (n *JoinNode) Evaluate() error { return nil }
func (n *JoinNode) Reset()          { *n = JoinNode{} }

// FFICallNode represents a foreign function interface call.
// Must be pointerless.
type FFICallNode struct {
	FunctionRef StringRef
	ArgsRef     uint32
	NextRef     uint32
}

func (n *FFICallNode) Type() NodeType  { return TypeFFICall }
func (n *FFICallNode) Evaluate() error { return nil }
func (n *FFICallNode) Reset()          { *n = FFICallNode{} }

// Object pooling with sync.Pool
var (
	logicalStepPool = sync.Pool{New: func() any { return &LogicalStepNode{} }}
	joinPool        = sync.Pool{New: func() any { return &JoinNode{} }}
	ffiCallPool     = sync.Pool{New: func() any { return &FFICallNode{} }}
)

// AcquireASTNode acquires a node of the specified type from the pool.
func AcquireASTNode(t NodeType) ASTNode {
	switch t {
	case TypeLogicalStep:
		return logicalStepPool.Get().(*LogicalStepNode)
	case TypeJoin:
		return joinPool.Get().(*JoinNode)
	case TypeFFICall:
		return ffiCallPool.Get().(*FFICallNode)
	default:
		return nil
	}
}

// ReleaseASTNode wipes the node and returns it to the pool, preventing memory leaks and state corruption.
func ReleaseASTNode(n ASTNode) {
	if n == nil {
		return
	}
	t := n.Type()
	n.Reset() // Completely zero out the struct before returning it
	switch t {
	case TypeLogicalStep:
		logicalStepPool.Put(n)
	case TypeJoin:
		joinPool.Put(n)
	case TypeFFICall:
		ffiCallPool.Put(n)
	}
}
