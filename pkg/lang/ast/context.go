package ast

import (
	"sync"
)

// ASTContext manages the lifecycle and storage of Abstract Syntax Tree (AST) nodes.
// It employs a Data-Oriented Design (DOD) by storing nodes in flat, contiguous slices
// of pointerless structs. This strategy minimizes Garbage Collector (GC) pressure,
// reduces heap fragmentation, and improves cache locality during parsing and compilation.
//
// The context is designed to be reused via a sync.Pool, where the Reset() method
// prepares the internal buffers for the next parsing phase without reallocating
// the underlying memory.
type ASTContext struct {
	// StringBuffer holds all raw string data (identifiers, paths, literals) to avoid
	// excessive small string allocations.
	StringBuffer []byte

	// Primary node storage. Slices are indexed by NodeRef.
	// Note: The 0-th element in most slices is reserved for the NilNode (null) reference.
	ImportNodes            []ImportNode
	ResourceNodes          []ResourceBindingNode
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
	CommentNodes           []CommentNode

	// Child reference slices used to store lists of children for parent nodes
	// (e.g., statements in a workflow, pairs in a dictionary).
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
	CommentRefs          []NodeRef

	// Parallel slices for source location tracking.
	// These map 1:1 to their corresponding node slices.
	ResourceRanges   []Range
	StepRanges       []Range
	HandlerRanges    []Range
	WorkflowRanges   []Range
	CallRanges       []Range
	DictRanges       []Range
	ImportRanges     []Range
	AssignmentRanges []Range
}

// Reset clears the context for reuse. It truncates slices to their initial state
// (usually index 1 to preserve the NilNode reservation) without releasing the
// underlying capacity, effectively zeroing out the context for a new parsing task.
func (ctx *ASTContext) Reset() {
	ctx.StringBuffer = ctx.StringBuffer[:0]

	// Reset node slices, preserving the NilNode at index 0
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
	ctx.CommentNodes = ctx.CommentNodes[:1]

	// Reset reference slices to 0 length
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
	ctx.CommentRefs = ctx.CommentRefs[:0]

	// Reset source range slices, preserving NilNode alignment
	ctx.ResourceRanges = ctx.ResourceRanges[:1]
	ctx.StepRanges = ctx.StepRanges[:1]
	ctx.HandlerRanges = ctx.HandlerRanges[:1]
	ctx.WorkflowRanges = ctx.WorkflowRanges[:1]
	ctx.CallRanges = ctx.CallRanges[:1]
	ctx.DictRanges = ctx.DictRanges[:1]
	ctx.ImportRanges = ctx.ImportRanges[:1]
	ctx.AssignmentRanges = ctx.AssignmentRanges[:1]
}

// AddString appends a string to the internal buffer and returns a StringRef
// containing its byte offsets. This enables zero-allocation string retrieval.
func (ctx *ASTContext) AddString(s string) StringRef {
	start := uint32(len(ctx.StringBuffer))
	ctx.StringBuffer = append(ctx.StringBuffer, s...)
	end := uint32(len(ctx.StringBuffer))
	return StringRef{Start: start, End: end}
}

// GetString retrieves the original string from the buffer using the provided StringRef.
func (ctx *ASTContext) GetString(ref StringRef) string {
	if ref.Start >= ref.End || ref.End > uint32(len(ctx.StringBuffer)) {
		return ""
	}
	return string(ctx.StringBuffer[ref.Start:ref.End])
}

// AddImportNode appends an ImportNode and returns its stable reference.
func (ctx *ASTContext) AddImportNode(n ImportNode) NodeRef {
	idx := uint32(len(ctx.ImportNodes))
	ctx.ImportNodes = append(ctx.ImportNodes, n)
	ctx.ImportRanges = append(ctx.ImportRanges, Range{})
	return NodeRef(idx)
}

// SetImportRange updates the source location metadata for an import alias.
func (ctx *ASTContext) SetImportRange(ref NodeRef, r Range) {
	ctx.ImportRanges[ref] = r
}

// AddResourceNode appends a ResourceNode and allocates its corresponding source range.
func (ctx *ASTContext) AddResourceNode(n ResourceBindingNode) NodeRef {
	idx := uint32(len(ctx.ResourceNodes))
	ctx.ResourceNodes = append(ctx.ResourceNodes, n)
	ctx.ResourceRanges = append(ctx.ResourceRanges, Range{})
	return NodeRef(idx)
}

// SetResourceRange updates the source location metadata for a resource.
func (ctx *ASTContext) SetResourceRange(ref NodeRef, r Range) {
	ctx.ResourceRanges[ref] = r
}

// AddStepBindingNode appends a StepBindingNode and allocates its source range.
func (ctx *ASTContext) AddStepBindingNode(n StepBindingNode) NodeRef {
	idx := uint32(len(ctx.StepBindingNodes))
	ctx.StepBindingNodes = append(ctx.StepBindingNodes, n)
	ctx.StepRanges = append(ctx.StepRanges, Range{})
	return NodeRef(idx)
}

// SetStepRange updates the source location metadata for a step binding.
func (ctx *ASTContext) SetStepRange(ref NodeRef, r Range) {
	ctx.StepRanges[ref] = r
}

// AddFunctionRefNode appends a FunctionRefNode.
func (ctx *ASTContext) AddFunctionRefNode(n FunctionRefNode) NodeRef {
	idx := uint32(len(ctx.FunctionRefNodes))
	ctx.FunctionRefNodes = append(ctx.FunctionRefNodes, n)
	return NodeRef(idx)
}

// AddResourceRefNode appends a ResourceRefNode.
func (ctx *ASTContext) AddResourceRefNode(n ResourceRefNode) NodeRef {
	idx := uint32(len(ctx.ResourceRefNodes))
	ctx.ResourceRefNodes = append(ctx.ResourceRefNodes, n)
	return NodeRef(idx)
}

// AddResourceMappingNode appends a ResourceMappingNode.
func (ctx *ASTContext) AddResourceMappingNode(n ResourceMappingNode) NodeRef {
	idx := uint32(len(ctx.ResourceMappingNodes))
	ctx.ResourceMappingNodes = append(ctx.ResourceMappingNodes, n)
	return NodeRef(idx)
}

// AddHandlerNode appends a HandlerNode and allocates its source range.
func (ctx *ASTContext) AddHandlerNode(n HandlerNode) NodeRef {
	idx := uint32(len(ctx.HandlerNodes))
	ctx.HandlerNodes = append(ctx.HandlerNodes, n)
	ctx.HandlerRanges = append(ctx.HandlerRanges, Range{})
	return NodeRef(idx)
}

// SetHandlerRange updates the source location metadata for a handler.
func (ctx *ASTContext) SetHandlerRange(ref NodeRef, r Range) {
	ctx.HandlerRanges[ref] = r
}

// AddHandlerStatementNode appends a HandlerStatementNode.
func (ctx *ASTContext) AddHandlerStatementNode(n HandlerStatementNode) NodeRef {
	idx := uint32(len(ctx.HandlerStatementNodes))
	ctx.HandlerStatementNodes = append(ctx.HandlerStatementNodes, n)
	return NodeRef(idx)
}

// AddWorkflowNode appends a WorkflowNode and allocates its source range.
func (ctx *ASTContext) AddWorkflowNode(n WorkflowNode) NodeRef {
	idx := uint32(len(ctx.WorkflowNodes))
	ctx.WorkflowNodes = append(ctx.WorkflowNodes, n)
	ctx.WorkflowRanges = append(ctx.WorkflowRanges, Range{})
	return NodeRef(idx)
}

// SetWorkflowRange updates the source location metadata for a workflow.
func (ctx *ASTContext) SetWorkflowRange(ref NodeRef, r Range) {
	ctx.WorkflowRanges[ref] = r
}

// AddPipelineStatementNode appends a PipelineStatementNode.
func (ctx *ASTContext) AddPipelineStatementNode(n PipelineStatementNode) NodeRef {
	idx := uint32(len(ctx.PipelineStatementNodes))
	ctx.PipelineStatementNodes = append(ctx.PipelineStatementNodes, n)
	ctx.AssignmentRanges = append(ctx.AssignmentRanges, Range{})
	return NodeRef(idx)
}

// SetAssignmentRange updates the source location metadata for an assignment identifier.
func (ctx *ASTContext) SetAssignmentRange(ref NodeRef, r Range) {
	ctx.AssignmentRanges[ref] = r
}

// AddPipeChainNode appends a PipeChainNode.
func (ctx *ASTContext) AddPipeChainNode(n PipeChainNode) NodeRef {
	idx := uint32(len(ctx.PipeChainNodes))
	ctx.PipeChainNodes = append(ctx.PipeChainNodes, n)
	return NodeRef(idx)
}

// AddCallNode appends a CallNode and allocates its source range.
func (ctx *ASTContext) AddCallNode(n CallNode) NodeRef {
	idx := uint32(len(ctx.CallNodes))
	ctx.CallNodes = append(ctx.CallNodes, n)
	ctx.CallRanges = append(ctx.CallRanges, Range{})
	return NodeRef(idx)
}

// SetCallRange updates the source location metadata for a call or query block.
func (ctx *ASTContext) SetCallRange(ref NodeRef, r Range) {
	ctx.CallRanges[ref] = r
}

// AddDataframeNode appends a DataframeNode.
func (ctx *ASTContext) AddDataframeNode(n DataframeNode) NodeRef {
	idx := uint32(len(ctx.DataframeNodes))
	ctx.DataframeNodes = append(ctx.DataframeNodes, n)
	return NodeRef(idx)
}

// AddDictNode appends a DictNode.
func (ctx *ASTContext) AddDictNode(n DictNode) NodeRef {
	idx := uint32(len(ctx.DictNodes))
	ctx.DictNodes = append(ctx.DictNodes, n)
	ctx.DictRanges = append(ctx.DictRanges, Range{})
	return NodeRef(idx)
}

// SetDictRange updates the source location metadata for a dictionary.
func (ctx *ASTContext) SetDictRange(ref NodeRef, r Range) {
	ctx.DictRanges[ref] = r
}

// AddPairNode appends a PairNode.
func (ctx *ASTContext) AddPairNode(n PairNode) NodeRef {
	idx := uint32(len(ctx.PairNodes))
	ctx.PairNodes = append(ctx.PairNodes, n)
	return NodeRef(idx)
}

// AddListNode appends a ListNode.
func (ctx *ASTContext) AddListNode(n ListNode) NodeRef {
	idx := uint32(len(ctx.ListNodes))
	ctx.ListNodes = append(ctx.ListNodes, n)
	return NodeRef(idx)
}

// AddLiteralNode appends a LiteralNode.
func (ctx *ASTContext) AddLiteralNode(n LiteralNode) NodeRef {
	idx := uint32(len(ctx.LiteralNodes))
	ctx.LiteralNodes = append(ctx.LiteralNodes, n)
	return NodeRef(idx)
}

// AddCommentNode appends a CommentNode.
func (ctx *ASTContext) AddCommentNode(n CommentNode) NodeRef {
	idx := uint32(len(ctx.CommentNodes))
	ctx.CommentNodes = append(ctx.CommentNodes, n)
	return NodeRef(idx)
}

// AddImportRef appends a child reference to the import slice.
func (ctx *ASTContext) AddImportRef(r NodeRef) {
	ctx.ImportRefs = append(ctx.ImportRefs, r)
}

// AddResourceRef appends a child reference to the resource slice.
func (ctx *ASTContext) AddResourceRef(r NodeRef) {
	ctx.ResourceRefs = append(ctx.ResourceRefs, r)
}

// AddStepRef appends a child reference to the step binding slice.
func (ctx *ASTContext) AddStepRef(r NodeRef) {
	ctx.StepRefs = append(ctx.StepRefs, r)
}

// AddHandlerRef appends a child reference to the handler slice.
func (ctx *ASTContext) AddHandlerRef(r NodeRef) {
	ctx.HandlerRefs = append(ctx.HandlerRefs, r)
}

// AddWorkflowRef appends a child reference to the workflow slice.
func (ctx *ASTContext) AddWorkflowRef(r NodeRef) {
	ctx.WorkflowRefs = append(ctx.WorkflowRefs, r)
}

// AddStatementRef appends a child reference to the pipeline statement slice.
func (ctx *ASTContext) AddStatementRef(r NodeRef) {
	ctx.StatementRefs = append(ctx.StatementRefs, r)
}

// AddHandlerStatementRef appends a child reference to the handler statement slice.
func (ctx *ASTContext) AddHandlerStatementRef(r NodeRef) {
	ctx.HandlerStatementRefs = append(ctx.HandlerStatementRefs, r)
}

// AddCallRef appends a child reference to the call slice.
func (ctx *ASTContext) AddCallRef(r NodeRef) {
	ctx.CallRefs = append(ctx.CallRefs, r)
}

// AddMappingRef appends a child reference to the resource mapping slice.
func (ctx *ASTContext) AddMappingRef(r NodeRef) {
	ctx.MappingRefs = append(ctx.MappingRefs, r)
}

// AddDictRef appends a child reference to the dictionary slice.
func (ctx *ASTContext) AddDictRef(r NodeRef) {
	ctx.DictRefs = append(ctx.DictRefs, r)
}

// AddPairRef appends a child reference to the pair slice.
func (ctx *ASTContext) AddPairRef(r NodeRef) {
	ctx.PairRefs = append(ctx.PairRefs, r)
}

// AddLiteralRef appends a child reference to the literal slice.
func (ctx *ASTContext) AddLiteralRef(r NodeRef) {
	ctx.LiteralRefs = append(ctx.LiteralRefs, r)
}

// AddCommentRef appends a child reference to the comment slice.
func (ctx *ASTContext) AddCommentRef(r NodeRef) {
	ctx.CommentRefs = append(ctx.CommentRefs, r)
}

// astContextPool is a global pool for ASTContext instances to minimize allocations.
var astContextPool = sync.Pool{
	New: func() any {
		return &ASTContext{
			StringBuffer: make([]byte, 0, 4096),
			// Initialize node slices with an empty element to reserve index 0 for NilNode.
			ImportNodes:            []ImportNode{{}},
			ResourceNodes:          []ResourceBindingNode{{}},
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
			CommentNodes:           []CommentNode{{}},

			// Pre-allocate reference slices with reasonable default capacities.
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
			CommentRefs:          make([]NodeRef, 0, 16),

			// Initialize range slices with an empty element for NilNode alignment.
			ResourceRanges:   []Range{{}},
			StepRanges:       []Range{{}},
			HandlerRanges:    []Range{{}},
			WorkflowRanges:   []Range{{}},
			CallRanges:       []Range{{}},
			DictRanges:       []Range{{}},
			ImportRanges:     []Range{{}},
			AssignmentRanges: []Range{{}},
		}
	},
}

// AcquireASTContext retrieves a clean ASTContext from the global pool.
func AcquireASTContext() *ASTContext {
	return astContextPool.Get().(*ASTContext)
}

// ReleaseASTContext returns an ASTContext to the pool after resetting its buffers.
func ReleaseASTContext(ctx *ASTContext) {
	ctx.Reset()
	astContextPool.Put(ctx)
}
