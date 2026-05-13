package ast

type LiteralType uint8

const (
	LiteralString LiteralType = iota
	LiteralInt
	LiteralFloat
	LiteralBool
	LiteralNull
	LiteralDict
	LiteralList
)

// NodeRef represents an index into the respective slice in ASTContext.
type NodeRef uint32

const (
	NilNode NodeRef = 0
)

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

// ResourceBindingNode represents a resource binding.
type ResourceBindingNode struct {
	NameRef     StringRef
	FunctionRef NodeRef // Index in FunctionRefNodes
}

// StepBindingNode represents a step binding.
type StepBindingNode struct {
	NameRef     StringRef
	FunctionRef NodeRef // Index in FunctionRefNodes
}

// FunctionRefNode represents a module.function reference with optional resource mapping and config.
type FunctionRefNode struct {
	ModuleRef       StringRef
	NameRef         StringRef
	ResourcesRefRef NodeRef // Index in ResourceRefNodes
	ConfigRef       NodeRef // Index in DictNodes
}

// ResourceRefNode represents a list of resource mappings <key=val, ...>.
type ResourceRefNode struct {
	MappingsRefStart uint32
	MappingsRefEnd   uint32
}

// ResourceMappingNode represents a single key=value mapping in a resource reference.
type ResourceMappingNode struct {
	KeyRef   StringRef
	ValueRef StringRef
}

// HandlerNode represents an error/event handler.
type HandlerNode struct {
	NameRef                   StringRef
	HandlerStatementRefsStart uint32
	HandlerStatementRefsEnd   uint32
}

// HandlerStatementNode represents a statement in a handler, which may have a catch-all '*'.
type HandlerStatementNode struct {
	IsCatchAll bool    // '*' was used, meaning catch errors, exceptions, dataframe and other failures.
	StmtRef    NodeRef // Index in PipelineStatementNodes
}

// WorkflowNode represents a workflow definition.
type WorkflowNode struct {
	NameRef            StringRef
	TrapRef            StringRef // Optional trap handler name
	StatementRefsStart uint32
	StatementRefsEnd   uint32
}

// PipelineStatementNode represents a single line in a workflow/handler.
type PipelineStatementNode struct {
	ExprRef       NodeRef   // Index in PipeChainNodes or DataframeNodes
	AssignmentRef StringRef // Optional > variable
}

// PipeChainNode represents a sequence of calls.
type PipeChainNode struct {
	CallRefsStart uint32
	CallRefsEnd   uint32
}

// CallNode represents a step call (bound or anonymous) or a query block.
type CallNode struct {
	NameRef      StringRef // For bound steps: name of the step.
	FunctionRef  NodeRef   // For anonymous steps: reference to FunctionRefNode.
	DataframeRef NodeRef   // For dataframe literals in pipelines.
	QueryRef     StringRef // For PRQL blocks: the query string.
	TrapRef      StringRef // Optional ?handler name.
	IsPrql       bool      // True if this is a PRQL query block.
}

// DataframeNode represents a constant dataframe [ { ... }, { ... } ].
type DataframeNode struct {
	DictRefsStart uint32
	DictRefsEnd   uint32
}

// DictNode represents a { key: value } structure.
type DictNode struct {
	PairRefsStart uint32
	PairRefsEnd   uint32
}

// PairNode represents a key-value pair in a dictionary.
type PairNode struct {
	KeyRef   StringRef
	ValueRef NodeRef // Index in LiteralNodes
}

// ListNode represents a [ val1, val2 ] list.
type ListNode struct {
	LiteralRefsStart uint32
	LiteralRefsEnd   uint32
}

// LiteralNode represents a primitive value or a nested structure.
type LiteralNode struct {
	Type     LiteralType
	ValueRef StringRef // For primitive types
	Ref      NodeRef   // For Dict or List
}

// CommentNode represents a block comment.
type CommentNode struct {
	ValueRef StringRef
}

// ProgramNode is the root of the AST.
type ProgramNode struct {
	ImportRefsStart   uint32
	ImportRefsEnd     uint32
	ResourceRefsStart uint32
	ResourceRefsEnd   uint32
	StepRefsStart     uint32
	StepRefsEnd       uint32
	HandlerRefsStart  uint32
	HandlerRefsEnd    uint32
	WorkflowRefsStart uint32
	WorkflowRefsEnd   uint32
	CommentRefsStart  uint32
	CommentRefsEnd    uint32
}
