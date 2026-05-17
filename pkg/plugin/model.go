package plugin

import (
	"time"

	"github.com/galgotech/heddle-lang/pkg/schema"
)

// Action types for Arrow Flight
const (
	ActionRegisterPlugin  = "register-plugin"
	ActionPluginHeartbeat = "plugin-heartbeat"
)

type StepResponseStatus string

const (
	StepResponseSuccess StepResponseStatus = "SUCCESS"
	StepResponseError   StepResponseStatus = "FAILED"
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

// ExecuteStepResponse contains the result of a plugin task execution.
type ExecuteStepResponse struct {
	TaskID        string             `json:"task_id"`
	Status        StepResponseStatus `json:"status"`
	ErrorMessage  string             `json:"error_message,omitempty"`
	OutputHandles map[string]string  `json:"output_handles,omitempty"`
	DirtyHandles  map[string]string  `json:"dirty_handles,omitempty"`
}

// Heartbeat is sent periodically by plugins to the worker.
type Heartbeat struct {
	Namespace string    `json:"namespace"`
	Timestamp time.Time `json:"timestamp"`
	Status    string    `json:"status"`
}

// ExecuteStepRequest encapsulates the metadata for a task delegated to a plugin.
type ExecuteStepRequest struct {
	WorkflowID    string            `json:"workflow_id"`
	TaskID        string            `json:"task_id"`
	StepName      string            `json:"step_name"`
	ResourceId    string            `json:"resource_id,omitempty"`
	ConfigJSON    string            `json:"config_json,omitempty"`
	InputHandles  map[string]string `json:"input_handles"`
	OutputHandles map[string]string `json:"output_handles"`
}

// SDKPluginStepDefinition represents step information passed from the SDK plugin.
type SDKPluginStepDefinition struct {
	Name          string                          `json:"name"`
	Config        *schema.ResourceAndConfigSchema `json:"config,omitempty"`
	Input         *schema.FrameSchema             `json:"input,omitempty"`
	Output        *schema.FrameSchema             `json:"output,omitempty"`
	Documentation string                          `json:"documentation,omitempty"`
	SourceFile    string                          `json:"source_file,omitempty"`
	SourceLine    int                             `json:"source_line,omitempty"`
}

// SDKPluginResourceDefinition represents resource information passed from the SDK plugin.
type SDKPluginResourceDefinition struct {
	Name          string                          `json:"name"`
	Config        *schema.ResourceAndConfigSchema `json:"config,omitempty"`
	Documentation string                          `json:"documentation,omitempty"`
	SourceFile    string                          `json:"source_file,omitempty"`
	SourceLine    int                             `json:"source_line,omitempty"`
}

// SDKPluginDefinitions represents the step and resource definitions imported/passed from an SDK plugin.
type SDKPluginDefinitions struct {
	Namespace string                        `json:"namespace"`
	Steps     []SDKPluginStepDefinition     `json:"steps"`
	Resources []SDKPluginResourceDefinition `json:"resources"`
}
