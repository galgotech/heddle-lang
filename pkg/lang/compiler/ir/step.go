package ir

// StepInstruction represents a single unit of execution in a Heddle workflow.
// It maps to a function in a host language (Python, Rust, Node.js).
type StepInstruction struct {
	BaseInstruction

	// DefinitionName is the name of the step as defined in the source (e.g. 'extract').
	DefinitionName string `json:"definition_name"`

	// Call represents the implementation mapping: [module, function].
	Call []string `json:"call"`

	// Resources maps configuration keys to resource instance names.
	Resources map[string]string `json:"resources"`

	// Config contains key-value pairs for step configuration.
	Config map[string]any `json:"config"`

	// InputType and OutputType specify the Arrow schema names for data flow.
	InputType  []string `json:"input_type"`
	OutputType []string `json:"output_type"`

	// Next is the list of IDs for subsequent instructions in the DAG.
	Next []string `json:"next,omitempty"`

	// Parents is the list of IDs for instructions that must complete before this step.
	Parents []string `json:"parents,omitempty"`

	// Assignment is the variable name to store the output of this step.
	Assignment string `json:"assignment,omitempty"`

	// Handler is the ID of the instruction to execute if this step fails.
	Handler string `json:"handler,omitempty"`

	// HandlerRedirectData indicates if the handler should receive the same data as the step.
	HandlerRedirectData bool `json:"handler_redirect_data"`
}
