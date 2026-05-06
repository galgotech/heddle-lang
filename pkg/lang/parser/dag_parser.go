package parser

import (
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
)

// ParseDAG is an example snippet demonstrating how the Recursive Descent Parser
// will request nodes from the pool, use them polymorphically, and release them.
func (p *Parser) ParseDAG() ast.ASTNode {
	// 1. Acquire a node from the pool without allocating on the heap
	// (The pool returns a pointer to a pointerless struct, hiding it from GC scanning)
	node := ast.AcquireASTNode(ast.TypeLogicalStep)

	// 2. Cast and populate
	if ls, ok := node.(*ast.LogicalStepNode); ok {
		ls.NameRef = p.ctx.AddString("ExampleStep")
		ls.NextRef = 0 // In a real parser, we would parse the next node
	}

	// 3. Polymorphic use
	_ = node.Evaluate()

	return node
}

// ReleaseDAG demonstrates safe release of an entire DAG tree
func ReleaseDAG(node ast.ASTNode) {
	if node == nil {
		return
	}

	// If nodes had pointers to children, we would recursively release them here.
	// Since we use pointerless structs with refs/indices, the actual traversal
	// would depend on an external graph structure or an array of children.
	// For this example snippet, we just release the single node.

	ast.ReleaseASTNode(node)
}
