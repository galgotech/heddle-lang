package ir

// FlowInstruction represents a high-level workflow definition (DAG).
type FlowInstruction struct {
	BaseInstruction

	// Name is the identifier for the workflow.
	Name string `json:"name"`

	// Heads are the starting points (Step IDs) of the workflow's execution paths.
	Heads []string `json:"heads"`

	// Handler is the ID of the global error handler for the workflow.
	Handler string `json:"handler,omitempty"`
}
