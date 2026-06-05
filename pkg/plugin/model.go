package plugin

import (
	"time"

	"github.com/galgotech/heddle-lang/pkg/schema"
)

// Action types for Arrow Flight
const (
	ActionRegisterPlugin  = "register-plugin"
	ActionPluginHeartbeat = "plugin-heartbeat"
	ActionResolveSchema   = "resolve-schema"
)

// ResolveSchemaRequest is sent by the worker to ask a plugin to resolve a dynamic schema
// based on a specific configuration instance.
type ResolveSchemaRequest struct {
	StepName   string `json:"step_name"`
	ConfigJSON string `json:"config_json"`
}

// ResolveSchemaResponse contains the resolved schemas for a dynamic step.
type ResolveSchemaResponse struct {
	Input  schema.FrameSchema `json:"input,omitempty"`
	Output schema.FrameSchema `json:"output,omitempty"`
	Error  string             `json:"error,omitempty"`
}

type StepResponseStatus string

const (
	StepResponseSuccess StepResponseStatus = "SUCCESS"
	StepResponseError   StepResponseStatus = "FAILED"
)

// PluginRegistration contains metadata sent by a plugin when it registers with a worker.
type PluginRegistration struct {
	Namespace string                        `json:"namespace"`
	Language  string                        `json:"language"`
	Version   string                        `json:"version"`
	Resources map[string]schema.FieldSchema `json:"resources,omitempty"`
	Schemas   map[string]schema.StepSchemas `json:"schemas,omitempty"`
}

// ResourceDefinition represents the metadata and configuration for a dynamic resource.
type ResourceDefinition struct {
	Type   string         `json:"type"`
	Config map[string]any `json:"config"`
}

// ExecuteStepRequest encapsulates the metadata for a task delegated to a plugin.
type ExecuteStepRequest struct {
	WorkflowID  string                        `json:"workflow_id"`
	TaskID      string                        `json:"task_id"`
	StepName    string                        `json:"step_name"`
	ConfigJSON  string                        `json:"config_json,omitempty"`
	InputRef    map[string]string             `json:"input_ref"`
	ResourceRef string                        `json:"resource_ref,omitempty"`
	Resources   map[string]ResourceDefinition `json:"resources,omitempty"`
}

// ExecuteStepResponse contains the result of a plugin task execution.
type ExecuteStepResponse struct {
	TaskID       string             `json:"task_id"`
	Status       StepResponseStatus `json:"status"`
	ErrorMessage string             `json:"error_message,omitempty"`
	OutputRef    map[string]string  `json:"output_ref,omitempty"`
}

// Heartbeat is sent periodically by plugins to the worker.
type Heartbeat struct {
	Namespace string    `json:"namespace"`
	Timestamp time.Time `json:"timestamp"`
	Status    string    `json:"status"`
}

// SDKPluginStepDefinition represents step information passed from the SDK plugin.
type SDKPluginStepDefinition struct {
	Name          string              `json:"name"`
	Config        *schema.FieldSchema `json:"config,omitempty"`
	Input         *schema.FrameSchema `json:"input,omitempty"`
	Output        *schema.FrameSchema `json:"output,omitempty"`
	Documentation string              `json:"documentation,omitempty"`
	SourceFile    string              `json:"source_file,omitempty"`
	SourceLine    int                 `json:"source_line,omitempty"`
}

// SDKPluginResourceDefinition represents resource information passed from the SDK plugin.
type SDKPluginResourceDefinition struct {
	Name          string              `json:"name"`
	Config        *schema.FieldSchema `json:"config,omitempty"`
	Documentation string              `json:"documentation,omitempty"`
	SourceFile    string              `json:"source_file,omitempty"`
	SourceLine    int                 `json:"source_line,omitempty"`
}

// SDKPluginDefinitions represents the step and resource definitions imported/passed from an SDK plugin.
type SDKPluginDefinitions struct {
	Namespace string                        `json:"namespace"`
	Steps     []SDKPluginStepDefinition     `json:"steps"`
	Resources []SDKPluginResourceDefinition `json:"resources"`
}
