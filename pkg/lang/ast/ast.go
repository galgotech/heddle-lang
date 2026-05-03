package ast

// NodeRef represents an index into the respective slice in ASTContext.
type NodeRef uint32

// StringRef represents a start and end index into the byte slice in ASTContext.
type StringRef struct {
	Start uint32
	End   uint32
}

// Position represents a line and column in the source file.
type Position struct {
	Line uint32
	Col  uint32
}

// Range represents a start and end position in the source file.
type Range struct {
	Start Position
	End   Position
}

// ImportNode represents an import statement.
type ImportNode struct {
	PathRef  StringRef
	AliasRef StringRef
}

// SchemaNode represents a schema definition.
type SchemaNode struct {
	NameRef  StringRef
	BlockRef NodeRef // Index in SchemaBlockNodes
	RefRef   NodeRef // Index in SchemaRefNodes
}

// SchemaBlockNode represents a structured schema body.
type SchemaBlockNode struct {
	FieldRefsStart uint32
	FieldRefsEnd   uint32
}

// SchemaFieldNode represents a field in a schema block.
type SchemaFieldNode struct {
	NameRef  StringRef
	TypeRef  StringRef // Primitive type name
	BlockRef NodeRef   // Optional nested block
}

// SchemaRefNode represents a reference to a schema (Module.Name).
type SchemaRefNode struct {
	ModuleRef StringRef
	NameRef   StringRef
}

// ResourceNode represents a resource binding.
type ResourceNode struct {
	NameRef StringRef
	RefRef  NodeRef // Index in FunctionRefNodes
}

// StepBindingNode represents a step binding with signature.
type StepBindingNode struct {
	NameRef      StringRef
	SignatureRef NodeRef // Index in StepSignatureNodes
	RefRef       NodeRef // Index in FunctionRefNodes
}

// StepSignatureNode represents an input -> output contract.
type StepSignatureNode struct {
	InputRef  NodeRef // Index in SchemaRefNodes or Void (special value)
	OutputRef NodeRef // Index in SchemaRefNodes or Void
}

// FunctionRefNode represents a module.function reference with optional config.
type FunctionRefNode struct {
	ModuleRef StringRef
	NameRef   StringRef
	// Optional resource/config could be added here
}

// HandlerNode represents an error/event handler.
type HandlerNode struct {
	NameRef           StringRef
	StatementRefsStart uint32
	StatementRefsEnd   uint32
}

// WorkflowNode represents a workflow definition.
type WorkflowNode struct {
	NameRef           StringRef
	TrapRef           StringRef // Optional trap handler name
	StatementRefsStart uint32
	StatementRefsEnd   uint32
}

// PipelineStatementNode represents a single line in a workflow/handler.
type PipelineStatementNode struct {
	ExprRef      NodeRef // Index in PipeChainNodes or DataframeNodes
	AssignmentRef StringRef // Optional > variable
}

// PipeChainNode represents a sequence of step calls.
type PipeChainNode struct {
	CallRefsStart uint32
	CallRefsEnd   uint32
}

// CallNode represents a step call with optional trap.
type CallNode struct {
	NameRef StringRef
	TrapRef StringRef // Optional ?handler
}

// ProgramNode is the root of the AST.
type ProgramNode struct {
	ImportRefsStart   uint32
	ImportRefsEnd     uint32
	SchemaRefsStart   uint32
	SchemaRefsEnd     uint32
	ResourceRefsStart uint32
	ResourceRefsEnd   uint32
	StepRefsStart     uint32
	StepRefsEnd       uint32
	HandlerRefsStart  uint32
	HandlerRefsEnd    uint32
	WorkflowRefsStart uint32
	WorkflowRefsEnd   uint32
}
