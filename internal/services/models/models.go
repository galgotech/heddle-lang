package models

import (
	"time"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

// Action types for Control Plane Arrow Flight
const (
	ActionRegisterWorker     = "register-worker"
	ActionHeartbeat          = "heartbeat"
	ActionSubmitWorkflow     = "submit-workflow"
	ActionUpdateCapabilities = "update-capabilities"
	ActionPurgeWorkflow      = "purge-workflow"
	ActionPurgeAck           = "purge-ack"
	ActionGetRegistry        = "get-registry"
)

// RegistryInfo contains the metadata about all registered steps in the cluster.
type RegistryInfo struct {
	Steps map[string]schema.StepSchemas `json:"steps"`
}

// Task Statuses
const (
	TaskStatusSuccess = "SUCCESS"
	TaskStatusFailed  = "FAILED"
)

// Standard type names
const (
	VoidType = "void"
)

// WorkerRegistration contains metadata sent by a worker when it registers with the Control Plane.
type WorkerRegistration struct {
	Address string `json:"address"`
}

// WorkerCapabilitiesUpdate contains the updated list of capabilities for a worker.
type WorkerCapabilitiesUpdate struct {
	Capabilities []string                      `json:"capabilities"`
	Schemas      map[string]schema.StepSchemas `json:"schemas,omitempty"`
}

// StepExecutionTask represents a single IR step dispatched to a worker.
type StepExecutionTask struct {
	WorkflowID     string              `json:"workflow_id"`
	TaskID         string              `json:"task_id"`
	PreviousTaskID string              `json:"previous_task_id,omitempty"`
	Step           *ir.StepInstruction `json:"step"`
}

// WorkerHeartbeat is sent periodically by workers to the Control Plane.
type WorkerHeartbeat struct {
	Timestamp time.Time `json:"timestamp"`
	Load      int       `json:"load"` // Current number of active tasks
}

// Task represents a unit of work dispatched to a worker.
type Task struct {
	ID             string      `json:"id"`
	Program        *ir.Program `json:"program"`
	TargetWorkflow string      `json:"target_workflow,omitempty"`
}

// TaskResult is the response from a worker after executing a task.
type TaskResult struct {
	TaskID       string `json:"task_id"`
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// WorkflowSubmission contains the source code of a Heddle program to be compiled and executed.
type WorkflowSubmission struct {
	Source       string `json:"source"`
	WorkflowName string `json:"workflow_name,omitempty"`
}

// WorkflowPurge is sent by the control plane to a worker after workflow termination.
type WorkflowPurge struct {
	WorkflowID string `json:"workflow_id"`
}

// PurgeAck is sent by the worker to the control plane after executing a purge.
type PurgeAck struct {
	WorkflowID string `json:"workflow_id"`
	WorkerID   string `json:"worker_id"`
}

// ControlMessage wraps any control directive sent from the CP to a worker
// via the DoExchange AppMetadata side-channel.
type ControlMessage struct {
	Type      string         `json:"type"`
	PurgeData *WorkflowPurge `json:"purge,omitempty"`
	PurgeAck  *PurgeAck      `json:"purge_ack,omitempty"`
}
