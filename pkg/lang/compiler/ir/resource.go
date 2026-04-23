package ir

// ResourceInstruction defines a connection to an external system.
type ResourceInstruction struct {
	BaseInstruction

	// Name is the identifier for the resource.
	Name string `json:"name"`

	// Provider represents the implementation mapping: [module, function].
	Provider []string `json:"provider"`

	// Config contains parameters for the resource provider.
	Config map[string]any `json:"config"`
}
