package ast

import (
	"sync"
)

// ASTContext holds all the data for an AST, storing it in flat slices to avoid GC overhead.
type ASTContext struct {
	StringBuffer []byte

	// Node slices
	ImportNodes            []ImportNode
	SchemaNodes            []SchemaNode
	SchemaBlockNodes       []SchemaBlockNode
	SchemaFieldNodes       []SchemaFieldNode
	SchemaRefNodes         []SchemaRefNode
	ResourceNodes          []ResourceNode
	StepBindingNodes       []StepBindingNode
	StepSignatureNodes     []StepSignatureNode
	FunctionRefNodes       []FunctionRefNode
	HandlerNodes           []HandlerNode
	WorkflowNodes          []WorkflowNode
	PipelineStatementNodes []PipelineStatementNode
	PipeChainNodes         []PipeChainNode
	CallNodes              []CallNode

	// Reference slices (to hold children indices)
	ImportRefs    []NodeRef
	SchemaRefs    []NodeRef
	ResourceRefs  []NodeRef
	StepRefs      []NodeRef
	HandlerRefs   []NodeRef
	WorkflowRefs  []NodeRef
	StatementRefs []NodeRef
	FieldRefs     []NodeRef
	CallRefs      []NodeRef

	// Range slices (parallel to node slices)
	ResourceRanges []Range
	StepRanges     []Range
	HandlerRanges  []Range
	WorkflowRanges []Range
	CallRanges     []Range
	SchemaRanges   []Range
}

// Reset clears the context for reuse without reallocating the underlying arrays.
func (ctx *ASTContext) Reset() {
	ctx.StringBuffer = ctx.StringBuffer[:0]
	ctx.ImportNodes = ctx.ImportNodes[:1]
	ctx.SchemaNodes = ctx.SchemaNodes[:1]
	ctx.SchemaBlockNodes = ctx.SchemaBlockNodes[:1]
	ctx.SchemaFieldNodes = ctx.SchemaFieldNodes[:1]
	ctx.SchemaRefNodes = ctx.SchemaRefNodes[:1]
	ctx.ResourceNodes = ctx.ResourceNodes[:1]
	ctx.StepBindingNodes = ctx.StepBindingNodes[:1]
	ctx.StepSignatureNodes = ctx.StepSignatureNodes[:1]
	ctx.FunctionRefNodes = ctx.FunctionRefNodes[:1]
	ctx.HandlerNodes = ctx.HandlerNodes[:1]
	ctx.WorkflowNodes = ctx.WorkflowNodes[:1]
	ctx.PipelineStatementNodes = ctx.PipelineStatementNodes[:1]
	ctx.PipeChainNodes = ctx.PipeChainNodes[:1]
	ctx.CallNodes = ctx.CallNodes[:1]

	ctx.ImportRefs = ctx.ImportRefs[:0]
	ctx.SchemaRefs = ctx.SchemaRefs[:0]
	ctx.ResourceRefs = ctx.ResourceRefs[:0]
	ctx.StepRefs = ctx.StepRefs[:0]
	ctx.HandlerRefs = ctx.HandlerRefs[:0]
	ctx.WorkflowRefs = ctx.WorkflowRefs[:0]
	ctx.StatementRefs = ctx.StatementRefs[:0]
	ctx.FieldRefs = ctx.FieldRefs[:0]
	ctx.CallRefs = ctx.CallRefs[:0]

	ctx.ResourceRanges = ctx.ResourceRanges[:1]
	ctx.StepRanges = ctx.StepRanges[:1]
	ctx.HandlerRanges = ctx.HandlerRanges[:1]
	ctx.WorkflowRanges = ctx.WorkflowRanges[:1]
	ctx.CallRanges = ctx.CallRanges[:1]
	ctx.SchemaRanges = ctx.SchemaRanges[:1]
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

// Helper methods to add nodes and refs
func (ctx *ASTContext) AddImportNode(n ImportNode) NodeRef {
	idx := uint32(len(ctx.ImportNodes))
	ctx.ImportNodes = append(ctx.ImportNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddSchemaNode(n SchemaNode) NodeRef {
	idx := uint32(len(ctx.SchemaNodes))
	ctx.SchemaNodes = append(ctx.SchemaNodes, n)
	ctx.SchemaRanges = append(ctx.SchemaRanges, Range{})
	return NodeRef(idx)
}

func (ctx *ASTContext) SetSchemaRange(ref NodeRef, r Range) {
	ctx.SchemaRanges[ref] = r
}

func (ctx *ASTContext) AddSchemaBlockNode(n SchemaBlockNode) NodeRef {
	idx := uint32(len(ctx.SchemaBlockNodes))
	ctx.SchemaBlockNodes = append(ctx.SchemaBlockNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddSchemaFieldNode(n SchemaFieldNode) NodeRef {
	idx := uint32(len(ctx.SchemaFieldNodes))
	ctx.SchemaFieldNodes = append(ctx.SchemaFieldNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddSchemaRefNode(n SchemaRefNode) NodeRef {
	idx := uint32(len(ctx.SchemaRefNodes))
	ctx.SchemaRefNodes = append(ctx.SchemaRefNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddResourceNode(n ResourceNode) NodeRef {
	idx := uint32(len(ctx.ResourceNodes))
	ctx.ResourceNodes = append(ctx.ResourceNodes, n)
	ctx.ResourceRanges = append(ctx.ResourceRanges, Range{})
	return NodeRef(idx)
}

func (ctx *ASTContext) SetResourceRange(ref NodeRef, r Range) {
	ctx.ResourceRanges[ref] = r
}

func (ctx *ASTContext) AddStepBindingNode(n StepBindingNode) NodeRef {
	idx := uint32(len(ctx.StepBindingNodes))
	ctx.StepBindingNodes = append(ctx.StepBindingNodes, n)
	ctx.StepRanges = append(ctx.StepRanges, Range{})
	return NodeRef(idx)
}

func (ctx *ASTContext) SetStepRange(ref NodeRef, r Range) {
	ctx.StepRanges[ref] = r
}

func (ctx *ASTContext) AddStepSignatureNode(n StepSignatureNode) NodeRef {
	idx := uint32(len(ctx.StepSignatureNodes))
	ctx.StepSignatureNodes = append(ctx.StepSignatureNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddFunctionRefNode(n FunctionRefNode) NodeRef {
	idx := uint32(len(ctx.FunctionRefNodes))
	ctx.FunctionRefNodes = append(ctx.FunctionRefNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddHandlerNode(n HandlerNode) NodeRef {
	idx := uint32(len(ctx.HandlerNodes))
	ctx.HandlerNodes = append(ctx.HandlerNodes, n)
	ctx.HandlerRanges = append(ctx.HandlerRanges, Range{})
	return NodeRef(idx)
}

func (ctx *ASTContext) SetHandlerRange(ref NodeRef, r Range) {
	ctx.HandlerRanges[ref] = r
}

func (ctx *ASTContext) AddWorkflowNode(n WorkflowNode) NodeRef {
	idx := uint32(len(ctx.WorkflowNodes))
	ctx.WorkflowNodes = append(ctx.WorkflowNodes, n)
	ctx.WorkflowRanges = append(ctx.WorkflowRanges, Range{})
	return NodeRef(idx)
}

func (ctx *ASTContext) SetWorkflowRange(ref NodeRef, r Range) {
	ctx.WorkflowRanges[ref] = r
}

func (ctx *ASTContext) AddPipelineStatementNode(n PipelineStatementNode) NodeRef {
	idx := uint32(len(ctx.PipelineStatementNodes))
	ctx.PipelineStatementNodes = append(ctx.PipelineStatementNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddPipeChainNode(n PipeChainNode) NodeRef {
	idx := uint32(len(ctx.PipeChainNodes))
	ctx.PipeChainNodes = append(ctx.PipeChainNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddCallNode(n CallNode) NodeRef {
	idx := uint32(len(ctx.CallNodes))
	ctx.CallNodes = append(ctx.CallNodes, n)
	ctx.CallRanges = append(ctx.CallRanges, Range{})
	return NodeRef(idx)
}

func (ctx *ASTContext) SetCallRange(ref NodeRef, r Range) {
	ctx.CallRanges[ref] = r
}

// Helper methods to add refs
func (ctx *ASTContext) AddImportRef(r NodeRef)   { ctx.ImportRefs = append(ctx.ImportRefs, r) }
func (ctx *ASTContext) AddSchemaRef(r NodeRef)   { ctx.SchemaRefs = append(ctx.SchemaRefs, r) }
func (ctx *ASTContext) AddResourceRef(r NodeRef) { ctx.ResourceRefs = append(ctx.ResourceRefs, r) }
func (ctx *ASTContext) AddStepRef(r NodeRef)     { ctx.StepRefs = append(ctx.StepRefs, r) }
func (ctx *ASTContext) AddHandlerRef(r NodeRef)  { ctx.HandlerRefs = append(ctx.HandlerRefs, r) }
func (ctx *ASTContext) AddWorkflowRef(r NodeRef) { ctx.WorkflowRefs = append(ctx.WorkflowRefs, r) }
func (ctx *ASTContext) AddStatementRef(r NodeRef) {
	ctx.StatementRefs = append(ctx.StatementRefs, r)
}
func (ctx *ASTContext) AddFieldRef(r NodeRef) { ctx.FieldRefs = append(ctx.FieldRefs, r) }
func (ctx *ASTContext) AddCallRef(r NodeRef)  { ctx.CallRefs = append(ctx.CallRefs, r) }

var astContextPool = sync.Pool{
	New: func() interface{} {
		return &ASTContext{
			StringBuffer:           make([]byte, 0, 4096),
			ImportNodes:            []ImportNode{{}},
			SchemaNodes:            []SchemaNode{{}},
			SchemaBlockNodes:       []SchemaBlockNode{{}},
			SchemaFieldNodes:       []SchemaFieldNode{{}},
			SchemaRefNodes:         []SchemaRefNode{{}},
			ResourceNodes:          []ResourceNode{{}},
			StepBindingNodes:       []StepBindingNode{{}},
			StepSignatureNodes:     []StepSignatureNode{{}},
			FunctionRefNodes:       []FunctionRefNode{{}},
			HandlerNodes:           []HandlerNode{{}},
			WorkflowNodes:          []WorkflowNode{{}},
			PipelineStatementNodes: []PipelineStatementNode{{}},
			PipeChainNodes:         []PipeChainNode{{}},
			CallNodes:              []CallNode{{}},

			ImportRefs:    make([]NodeRef, 0, 16),
			SchemaRefs:    make([]NodeRef, 0, 32),
			ResourceRefs:  make([]NodeRef, 0, 16),
			StepRefs:      make([]NodeRef, 0, 32),
			HandlerRefs:   make([]NodeRef, 0, 16),
			WorkflowRefs:  make([]NodeRef, 0, 16),
			StatementRefs: make([]NodeRef, 0, 256),
			FieldRefs:     make([]NodeRef, 0, 128),
			CallRefs:      make([]NodeRef, 0, 256),

			ResourceRanges: []Range{{}},
			StepRanges:     []Range{{}},
			HandlerRanges:  []Range{{}},
			WorkflowRanges: []Range{{}},
			CallRanges:     []Range{{}},
			SchemaRanges:   []Range{{}},
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
