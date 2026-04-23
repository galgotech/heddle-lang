package ir

// StepInstruction represents a single unit of execution in a Heddle workflow.
// It maps to a function in a host language (Python, Rust, Node.js).
type StepInstruction struct {
	BaseInstruction

	// DefinitionName is the name of the step as defined in the source (e.g. 'extract').
	DefinitionName string `json:"definition_name"`

	// Call represents the implementation mapping: [module, function].
	Call []string `json:"call"`

	// Config contains key-value pairs for step configuration.
	Config map[string]any `json:"config"`

	// Resources maps configuration keys to resource instance names.
	Resources map[string]string `json:"resources"`

	// InputType and OutputType specify the Arrow schema names for data flow.
	InputType  []string `json:"input_type"`
	OutputType []string `json:"output_type"`

	// Next is the ID of the subsequent instruction in the pipeline.
	Next string `json:"next,omitempty"`

	// Assignment is the variable name to store the output of this step.
	Assignment string `json:"assignment,omitempty"`

	// Handler is the ID of the instruction to execute if this step fails.
	Handler string `json:"handler,omitempty"`

	// HandlerRedirectData indicates if the handler should receive the same data as the step.
	HandlerRedirectData bool `json:"handler_redirect_data"`
}
