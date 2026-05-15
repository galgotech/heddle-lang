package plugin

import (
	"time"
)

// RouteType defines how a resource should be accessed.
type RouteType int

type StepResponseStatus string

const (
	StepResponseSuccess StepResponseStatus = "SUCCESS"
	StepResponseError   StepResponseStatus = "FAILED"
)

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

// ExecuteStepResponse contains the result of a plugin task execution.
type ExecuteStepResponse struct {
	TaskID        string             `json:"task_id"`
	Status        StepResponseStatus `json:"status"`
	ErrorMessage  string             `json:"error_message,omitempty"`
	OutputHandles map[string]string  `json:"output_handles,omitempty"`
	DirtyHandles  map[string]string  `json:"dirty_handles,omitempty"`
}
