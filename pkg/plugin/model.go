package plugin

import "github.com/galgotech/heddle-lang/pkg/schema"

// Action types for Arrow Flight
const (
	ActionRegisterPlugin  = "register-plugin"
	ActionPluginHeartbeat = "plugin-heartbeat"
)

// PluginRegistration contains metadata sent by a plugin when it registers with a worker.
type PluginRegistration struct {
	Namespace    string                                     `json:"namespace"`
	Language     string                                     `json:"language"`
	Version      string                                     `json:"version"`
	Capabilities []string                                   `json:"capabilities"`
	Resources    map[string]*schema.ResourceAndConfigSchema `json:"resources,omitempty"`
	Schemas      map[string]schema.StepSchemas              `json:"schemas,omitempty"`
}
