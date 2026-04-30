package fastast

import (
	"sync"
)

// ASTContext holds all the data for an AST, storing it in flat slices to avoid GC overhead.
type ASTContext struct {
	// Strings buffer to hold all string data
	StringBuffer []byte

	// Node slices
	TaskNodes []TaskNode
	DAGNodes  []DAGNode

	// References to tasks for DAGs
	TaskRefs []NodeRef

	// References to DAGs for the program
	DAGRefs []NodeRef
}

// Reset clears the context for reuse without reallocating the underlying arrays.
func (ctx *ASTContext) Reset() {
	ctx.StringBuffer = ctx.StringBuffer[:0]
	ctx.TaskNodes = ctx.TaskNodes[:0]
	ctx.DAGNodes = ctx.DAGNodes[:0]
	ctx.TaskRefs = ctx.TaskRefs[:0]
	ctx.DAGRefs = ctx.DAGRefs[:0]
}

// AddString appends a string to the buffer and returns its reference.
func (ctx *ASTContext) AddString(s string) StringRef {
	start := uint32(len(ctx.StringBuffer))
	ctx.StringBuffer = append(ctx.StringBuffer, s...)
	end := uint32(len(ctx.StringBuffer))
	return StringRef{Start: start, End: end}
}

// GetString retrieves a string from the buffer given a reference.
func (ctx *ASTContext) GetString(ref StringRef) string {
	if ref.Start >= ref.End || ref.End > uint32(len(ctx.StringBuffer)) {
		return ""
	}
	return string(ctx.StringBuffer[ref.Start:ref.End])
}

// AddTaskNode appends a TaskNode and returns its reference.
func (ctx *ASTContext) AddTaskNode(node TaskNode) NodeRef {
	idx := uint32(len(ctx.TaskNodes))
	ctx.TaskNodes = append(ctx.TaskNodes, node)
	return NodeRef(idx)
}

// AddDAGNode appends a DAGNode and returns its reference.
func (ctx *ASTContext) AddDAGNode(node DAGNode) NodeRef {
	idx := uint32(len(ctx.DAGNodes))
	ctx.DAGNodes = append(ctx.DAGNodes, node)
	return NodeRef(idx)
}

// AddTaskRef appends a TaskRef and returns its index in the TaskRefs slice.
func (ctx *ASTContext) AddTaskRef(ref NodeRef) uint32 {
	idx := uint32(len(ctx.TaskRefs))
	ctx.TaskRefs = append(ctx.TaskRefs, ref)
	return idx
}

// AddDAGRef appends a DAGRef and returns its index in the DAGRefs slice.
func (ctx *ASTContext) AddDAGRef(ref NodeRef) uint32 {
	idx := uint32(len(ctx.DAGRefs))
	ctx.DAGRefs = append(ctx.DAGRefs, ref)
	return idx
}

var astContextPool = sync.Pool{
	New: func() interface{} {
		return &ASTContext{
			StringBuffer: make([]byte, 0, 1024),
			TaskNodes:    make([]TaskNode, 0, 128),
			DAGNodes:     make([]DAGNode, 0, 32),
			TaskRefs:     make([]NodeRef, 0, 128),
			DAGRefs:      make([]NodeRef, 0, 32),
		}
	},
}

// AcquireASTContext gets a clean ASTContext from the pool.
func AcquireASTContext() *ASTContext {
	return astContextPool.Get().(*ASTContext)
}

// ReleaseASTContext returns an ASTContext to the pool after resetting it.
func ReleaseASTContext(ctx *ASTContext) {
	ctx.Reset()
	astContextPool.Put(ctx)
}
