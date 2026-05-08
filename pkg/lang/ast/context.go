package ast

import (
	"sync"
)

// ASTContext holds all the data for an AST, storing it in flat slices to avoid GC overhead.
type ASTContext struct {
	StringBuffer []byte

	// Node slices
	ImportNodes            []ImportNode
	ResourceNodes          []ResourceNode
	StepBindingNodes       []StepBindingNode
	FunctionRefNodes       []FunctionRefNode
	ResourceRefNodes       []ResourceRefNode
	ResourceMappingNodes   []ResourceMappingNode
	HandlerNodes           []HandlerNode
	HandlerStatementNodes  []HandlerStatementNode
	WorkflowNodes          []WorkflowNode
	PipelineStatementNodes []PipelineStatementNode
	PipeChainNodes         []PipeChainNode
	CallNodes              []CallNode
	DataframeNodes         []DataframeNode
	DictNodes              []DictNode
	PairNodes              []PairNode
	ListNodes              []ListNode
	LiteralNodes           []LiteralNode

	// Reference slices (to hold children indices)
	ImportRefs           []NodeRef
	ResourceRefs         []NodeRef
	StepRefs             []NodeRef
	HandlerRefs          []NodeRef
	WorkflowRefs         []NodeRef
	StatementRefs        []NodeRef
	HandlerStatementRefs []NodeRef
	CallRefs             []NodeRef
	MappingRefs          []NodeRef
	DictRefs             []NodeRef
	PairRefs             []NodeRef
	LiteralRefs          []NodeRef

	// Range slices (parallel to node slices)
	ResourceRanges []Range
	StepRanges     []Range
	HandlerRanges  []Range
	WorkflowRanges []Range
	CallRanges     []Range
}

// Reset clears the context for reuse without reallocating the underlying arrays.
func (ctx *ASTContext) Reset() {
	ctx.StringBuffer = ctx.StringBuffer[:0]
	ctx.ImportNodes = ctx.ImportNodes[:1]
	ctx.ResourceNodes = ctx.ResourceNodes[:1]
	ctx.StepBindingNodes = ctx.StepBindingNodes[:1]
	ctx.FunctionRefNodes = ctx.FunctionRefNodes[:1]
	ctx.ResourceRefNodes = ctx.ResourceRefNodes[:1]
	ctx.ResourceMappingNodes = ctx.ResourceMappingNodes[:1]
	ctx.HandlerNodes = ctx.HandlerNodes[:1]
	ctx.HandlerStatementNodes = ctx.HandlerStatementNodes[:1]
	ctx.WorkflowNodes = ctx.WorkflowNodes[:1]
	ctx.PipelineStatementNodes = ctx.PipelineStatementNodes[:1]
	ctx.PipeChainNodes = ctx.PipeChainNodes[:1]
	ctx.CallNodes = ctx.CallNodes[:1]
	ctx.DataframeNodes = ctx.DataframeNodes[:1]
	ctx.DictNodes = ctx.DictNodes[:1]
	ctx.PairNodes = ctx.PairNodes[:1]
	ctx.ListNodes = ctx.ListNodes[:1]
	ctx.LiteralNodes = ctx.LiteralNodes[:1]

	ctx.ImportRefs = ctx.ImportRefs[:0]
	ctx.ResourceRefs = ctx.ResourceRefs[:0]
	ctx.StepRefs = ctx.StepRefs[:0]
	ctx.HandlerRefs = ctx.HandlerRefs[:0]
	ctx.WorkflowRefs = ctx.WorkflowRefs[:0]
	ctx.StatementRefs = ctx.StatementRefs[:0]
	ctx.HandlerStatementRefs = ctx.HandlerStatementRefs[:0]
	ctx.CallRefs = ctx.CallRefs[:0]
	ctx.MappingRefs = ctx.MappingRefs[:0]
	ctx.DictRefs = ctx.DictRefs[:0]
	ctx.PairRefs = ctx.PairRefs[:0]
	ctx.LiteralRefs = ctx.LiteralRefs[:0]

	ctx.ResourceRanges = ctx.ResourceRanges[:1]
	ctx.StepRanges = ctx.StepRanges[:1]
	ctx.HandlerRanges = ctx.HandlerRanges[:1]
	ctx.WorkflowRanges = ctx.WorkflowRanges[:1]
	ctx.CallRanges = ctx.CallRanges[:1]
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

func (ctx *ASTContext) AddFunctionRefNode(n FunctionRefNode) NodeRef {
	idx := uint32(len(ctx.FunctionRefNodes))
	ctx.FunctionRefNodes = append(ctx.FunctionRefNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddResourceRefNode(n ResourceRefNode) NodeRef {
	idx := uint32(len(ctx.ResourceRefNodes))
	ctx.ResourceRefNodes = append(ctx.ResourceRefNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddResourceMappingNode(n ResourceMappingNode) NodeRef {
	idx := uint32(len(ctx.ResourceMappingNodes))
	ctx.ResourceMappingNodes = append(ctx.ResourceMappingNodes, n)
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

func (ctx *ASTContext) AddHandlerStatementNode(n HandlerStatementNode) NodeRef {
	idx := uint32(len(ctx.HandlerStatementNodes))
	ctx.HandlerStatementNodes = append(ctx.HandlerStatementNodes, n)
	return NodeRef(idx)
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

func (ctx *ASTContext) AddDataframeNode(n DataframeNode) NodeRef {
	idx := uint32(len(ctx.DataframeNodes))
	ctx.DataframeNodes = append(ctx.DataframeNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddDictNode(n DictNode) NodeRef {
	idx := uint32(len(ctx.DictNodes))
	ctx.DictNodes = append(ctx.DictNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddPairNode(n PairNode) NodeRef {
	idx := uint32(len(ctx.PairNodes))
	ctx.PairNodes = append(ctx.PairNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddListNode(n ListNode) NodeRef {
	idx := uint32(len(ctx.ListNodes))
	ctx.ListNodes = append(ctx.ListNodes, n)
	return NodeRef(idx)
}

func (ctx *ASTContext) AddLiteralNode(n LiteralNode) NodeRef {
	idx := uint32(len(ctx.LiteralNodes))
	ctx.LiteralNodes = append(ctx.LiteralNodes, n)
	return NodeRef(idx)
}

// Helper methods to add refs
func (ctx *ASTContext) AddImportRef(r NodeRef)   { ctx.ImportRefs = append(ctx.ImportRefs, r) }
func (ctx *ASTContext) AddResourceRef(r NodeRef) { ctx.ResourceRefs = append(ctx.ResourceRefs, r) }
func (ctx *ASTContext) AddStepRef(r NodeRef)     { ctx.StepRefs = append(ctx.StepRefs, r) }
func (ctx *ASTContext) AddHandlerRef(r NodeRef)  { ctx.HandlerRefs = append(ctx.HandlerRefs, r) }
func (ctx *ASTContext) AddWorkflowRef(r NodeRef) { ctx.WorkflowRefs = append(ctx.WorkflowRefs, r) }
func (ctx *ASTContext) AddStatementRef(r NodeRef) {
	ctx.StatementRefs = append(ctx.StatementRefs, r)
}
func (ctx *ASTContext) AddHandlerStatementRef(r NodeRef) {
	ctx.HandlerStatementRefs = append(ctx.HandlerStatementRefs, r)
}
func (ctx *ASTContext) AddCallRef(r NodeRef)    { ctx.CallRefs = append(ctx.CallRefs, r) }
func (ctx *ASTContext) AddMappingRef(r NodeRef) { ctx.MappingRefs = append(ctx.MappingRefs, r) }
func (ctx *ASTContext) AddDictRef(r NodeRef)    { ctx.DictRefs = append(ctx.DictRefs, r) }
func (ctx *ASTContext) AddPairRef(r NodeRef)    { ctx.PairRefs = append(ctx.PairRefs, r) }
func (ctx *ASTContext) AddLiteralRef(r NodeRef) { ctx.LiteralRefs = append(ctx.LiteralRefs, r) }

var astContextPool = sync.Pool{
	New: func() any {
		return &ASTContext{
			StringBuffer:           make([]byte, 0, 4096),
			ImportNodes:            []ImportNode{{}},
			ResourceNodes:          []ResourceNode{{}},
			StepBindingNodes:       []StepBindingNode{{}},
			FunctionRefNodes:       []FunctionRefNode{{}},
			ResourceRefNodes:       []ResourceRefNode{{}},
			ResourceMappingNodes:   []ResourceMappingNode{{}},
			HandlerNodes:           []HandlerNode{{}},
			HandlerStatementNodes:  []HandlerStatementNode{{}},
			WorkflowNodes:          []WorkflowNode{{}},
			PipelineStatementNodes: []PipelineStatementNode{{}},
			PipeChainNodes:         []PipeChainNode{{}},
			CallNodes:              []CallNode{{}},
			DataframeNodes:         []DataframeNode{{}},
			DictNodes:              []DictNode{{}},
			PairNodes:              []PairNode{{}},
			ListNodes:              []ListNode{{}},
			LiteralNodes:           []LiteralNode{{}},

			ImportRefs:           make([]NodeRef, 0, 16),
			ResourceRefs:         make([]NodeRef, 0, 16),
			StepRefs:             make([]NodeRef, 0, 32),
			HandlerRefs:          make([]NodeRef, 0, 16),
			WorkflowRefs:         make([]NodeRef, 0, 16),
			StatementRefs:        make([]NodeRef, 0, 256),
			HandlerStatementRefs: make([]NodeRef, 0, 256),
			CallRefs:             make([]NodeRef, 0, 256),
			MappingRefs:          make([]NodeRef, 0, 64),
			DictRefs:             make([]NodeRef, 0, 64),
			PairRefs:             make([]NodeRef, 0, 128),
			LiteralRefs:          make([]NodeRef, 0, 128),

			ResourceRanges: []Range{{}},
			StepRanges:     []Range{{}},
			HandlerRanges:  []Range{{}},
			WorkflowRanges: []Range{{}},
			CallRanges:     []Range{{}},
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
