package models

import (
	"time"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
)

// Action types for Control Plane Arrow Flight
const (
	ActionRegisterWorker     = "register-worker"
	ActionHeartbeat          = "heartbeat"
	ActionSubmitWorkflow     = "submit-workflow"
	ActionUpdateCapabilities = "update-capabilities"
)

// WorkerRegistration contains metadata sent by a worker when it registers with the Control Plane.
type WorkerRegistration struct {
	Address string `json:"address"`
}

// WorkerCapabilitiesUpdate contains the updated list of capabilities for a worker.
type WorkerCapabilitiesUpdate struct {
	Capabilities []string `json:"capabilities"`
}

// StepExecutionTask represents a single IR step dispatched to a worker.
type StepExecutionTask struct {
	WorkflowID string              `json:"workflow_id"`
	TaskID     string              `json:"task_id"`
	Step       *ir.StepInstruction `json:"step"`
}

// WorkerHeartbeat is sent periodically by workers to the Control Plane.
type WorkerHeartbeat struct {
	Timestamp time.Time `json:"timestamp"`
	Load      int       `json:"load"` // Current number of active tasks
}

// Task represents a unit of work dispatched to a worker.
type Task struct {
	ID      string      `json:"id"`
	Program *ir.Program `json:"program"`
}

// TaskResult is the response from a worker after executing a task.
type TaskResult struct {
	TaskID       string `json:"task_id"`
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// WorkflowSubmission contains the source code of a Heddle program to be compiled and executed.
type WorkflowSubmission struct {
	Source string `json:"source"`
}
