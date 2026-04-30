package fastast

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
}

// Reset clears the context for reuse without reallocating the underlying arrays.
func (ctx *ASTContext) Reset() {
	ctx.StringBuffer = ctx.StringBuffer[:0]
	ctx.ImportNodes = ctx.ImportNodes[:0]
	ctx.SchemaNodes = ctx.SchemaNodes[:0]
	ctx.SchemaBlockNodes = ctx.SchemaBlockNodes[:0]
	ctx.SchemaFieldNodes = ctx.SchemaFieldNodes[:0]
	ctx.SchemaRefNodes = ctx.SchemaRefNodes[:0]
	ctx.ResourceNodes = ctx.ResourceNodes[:0]
	ctx.StepBindingNodes = ctx.StepBindingNodes[:0]
	ctx.StepSignatureNodes = ctx.StepSignatureNodes[:0]
	ctx.FunctionRefNodes = ctx.FunctionRefNodes[:0]
	ctx.HandlerNodes = ctx.HandlerNodes[:0]
	ctx.WorkflowNodes = ctx.WorkflowNodes[:0]
	ctx.PipelineStatementNodes = ctx.PipelineStatementNodes[:0]
	ctx.PipeChainNodes = ctx.PipeChainNodes[:0]
	ctx.CallNodes = ctx.CallNodes[:0]

	ctx.ImportRefs = ctx.ImportRefs[:0]
	ctx.SchemaRefs = ctx.SchemaRefs[:0]
	ctx.ResourceRefs = ctx.ResourceRefs[:0]
	ctx.StepRefs = ctx.StepRefs[:0]
	ctx.HandlerRefs = ctx.HandlerRefs[:0]
	ctx.WorkflowRefs = ctx.WorkflowRefs[:0]
	ctx.StatementRefs = ctx.StatementRefs[:0]
	ctx.FieldRefs = ctx.FieldRefs[:0]
	ctx.CallRefs = ctx.CallRefs[:0]
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
	return NodeRef(idx)
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
	return NodeRef(idx)
}

func (ctx *ASTContext) AddStepBindingNode(n StepBindingNode) NodeRef {
	idx := uint32(len(ctx.StepBindingNodes))
	ctx.StepBindingNodes = append(ctx.StepBindingNodes, n)
	return NodeRef(idx)
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
	return NodeRef(idx)
}

func (ctx *ASTContext) AddWorkflowNode(n WorkflowNode) NodeRef {
	idx := uint32(len(ctx.WorkflowNodes))
	ctx.WorkflowNodes = append(ctx.WorkflowNodes, n)
	return NodeRef(idx)
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
	return NodeRef(idx)
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
			ImportNodes:            make([]ImportNode, 0, 16),
			SchemaNodes:            make([]SchemaNode, 0, 32),
			SchemaBlockNodes:       make([]SchemaBlockNode, 0, 32),
			SchemaFieldNodes:       make([]SchemaFieldNode, 0, 128),
			SchemaRefNodes:         make([]SchemaRefNode, 0, 64),
			ResourceNodes:          make([]ResourceNode, 0, 16),
			StepBindingNodes:       make([]StepBindingNode, 0, 32),
			StepSignatureNodes:     make([]StepSignatureNode, 0, 32),
			FunctionRefNodes:       make([]FunctionRefNode, 0, 32),
			HandlerNodes:           make([]HandlerNode, 0, 16),
			WorkflowNodes:          make([]WorkflowNode, 0, 16),
			PipelineStatementNodes: make([]PipelineStatementNode, 0, 128),
			PipeChainNodes:         make([]PipeChainNode, 0, 64),
			CallNodes:              make([]CallNode, 0, 256),

			ImportRefs:    make([]NodeRef, 0, 16),
			SchemaRefs:    make([]NodeRef, 0, 32),
			ResourceRefs:  make([]NodeRef, 0, 16),
			StepRefs:      make([]NodeRef, 0, 32),
			HandlerRefs:   make([]NodeRef, 0, 16),
			WorkflowRefs:  make([]NodeRef, 0, 16),
			StatementRefs: make([]NodeRef, 0, 256),
			FieldRefs:     make([]NodeRef, 0, 128),
			CallRefs:      make([]NodeRef, 0, 256),
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
