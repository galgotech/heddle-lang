package ir

// ImportInstruction represents an import statement in the IR.
type ImportInstruction struct {
	BaseInstruction
	Path  string `json:"path"`
	Alias string `json:"alias"`
}
