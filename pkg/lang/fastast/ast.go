package fastast

// NodeRef represents an index into the respective slice in ASTContext.
type NodeRef uint32

// StringRef represents a start and end index into the byte slice in ASTContext.
type StringRef struct {
	Start uint32
	End   uint32
}

// TaskNode represents a task in the pipeline. It is pointerless to evade GC.
type TaskNode struct {
	NameRef StringRef // Reference to the name of the task
	CommandRef StringRef // Reference to the command to run
	// In a real parser, we might have references to arguments or other properties
}

// DAGNode represents a Directed Acyclic Graph (DAG) composed of tasks.
// It is pointerless to evade GC.
type DAGNode struct {
	NameRef StringRef // Reference to the DAG name
	// Tasks represents a range of tasks in the []TaskRef slice in ASTContext.
	// Since we can't use slices in the node (which contain pointers to backing array),
	// we use start and end indices.
	TaskRefsStart uint32
	TaskRefsEnd   uint32
}

// ProgramNode is the root of the AST.
type ProgramNode struct {
	DAGRefsStart uint32
	DAGRefsEnd   uint32
}
