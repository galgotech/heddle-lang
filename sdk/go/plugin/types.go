package plugin

import "time"

// RouteType defines how a resource should be accessed.
type RouteType int

const (
	RouteTypeLocal  RouteType = 0 // Access via Unix sockets or shared memory
	RouteTypeRemote RouteType = 1 // Access via network gRPC/Flight RPC
)

// FlightTicket provides the metadata required for a worker to retrieve a resource.
type FlightTicket struct {
	RouteType  RouteType `json:"route_type"`
	Address    string    `json:"address"`
	ResourceId string    `json:"resource_id"`
}

// PluginRegistration contains metadata sent by a plugin when it registers with a worker.
type PluginRegistration struct {
	Namespace    string   `json:"namespace"`
	Language     string   `json:"language"`
	Version      string   `json:"version"`
	Capabilities []string `json:"capabilities"`
}

// Heartbeat is sent periodically by plugins to the worker.
type Heartbeat struct {
	Timestamp time.Time `json:"timestamp"`
	Status    string    `json:"status"`
}

// ExecuteStepRequest encapsulates the metadata for a task delegated to a plugin.
type ExecuteStepRequest struct {
	TaskID       string `json:"task_id"`
	StepName     string `json:"step_name"`
	ResourceId   string `json:"resource_id,omitempty"`
	ConfigJSON   string `json:"config_json,omitempty"`
	InputHandle  string `json:"input_handle"`
	OutputHandle string `json:"output_handle"`
}

// ExecuteStepResponse contains the result of a plugin task execution.
type ExecuteStepResponse struct {
	TaskID       string `json:"task_id"`
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message,omitempty"`
	OutputHandle string `json:"output_handle,omitempty"`
}

// Action types for Arrow Flight
const (
	ActionRegisterPlugin  = "register-plugin"
	ActionPluginHeartbeat = "plugin-heartbeat"
)
