package ir

// ProgramIR is the top-level container for a compiled Heddle program.
// It is designed to be fully serializable for transmission to workers.
type ProgramIR struct {
	BaseInstruction

	// Instructions is a flat registry of all instructions by their unique ID.
	Instructions map[string]interface{} `json:"instructions"`

	// Workflows lists the IDs of the entry points (FlowInstructions) in the program.
	Workflows []string `json:"workflows"`
}
