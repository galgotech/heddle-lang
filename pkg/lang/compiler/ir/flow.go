package ir

// FlowInstruction defines the high-level orchestration logic and DAG topology for a Heddle workflow.
type FlowInstruction struct {
	BaseInstruction

	// Name is the unique human-readable identifier for the workflow.
	Name string `json:"name"`

	// Heads lists the Step IDs that serve as the entry points for workflow execution.
	Heads []string `json:"heads"`

	// Handler identifies the Step ID of the global error handler for the workflow.
	Handler string `json:"handler,omitempty"`
}
