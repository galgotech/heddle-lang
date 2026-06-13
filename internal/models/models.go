package models

import (
	"context"
	"time"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

type contextKey string

const ControlSenderKey contextKey = "heddle-control-sender"
const UploadWaiterKey contextKey = "heddle-upload-waiter"

// ControlSender is a function type for sending control messages.
type ControlSender func(msg *ControlMessage) error

// UploadWaiter is a function type that blocks until a file upload for a task completes.
type UploadWaiter func(ctx context.Context, taskID string) (map[string]string, error)

// WithControlSender returns a new context containing the provided ControlSender.
func WithControlSender(ctx context.Context, sender ControlSender) context.Context {
	return context.WithValue(ctx, ControlSenderKey, sender)
}

// GetControlSender retrieves the ControlSender from the context.
func GetControlSender(ctx context.Context) ControlSender {
	if ctx == nil {
		return nil
	}
	if s, ok := ctx.Value(ControlSenderKey).(ControlSender); ok {
		return s
	}
	return nil
}

// WithUploadWaiter returns a new context containing the provided UploadWaiter.
func WithUploadWaiter(ctx context.Context, waiter UploadWaiter) context.Context {
	return context.WithValue(ctx, UploadWaiterKey, waiter)
}

// GetUploadWaiter retrieves the UploadWaiter from the context.
func GetUploadWaiter(ctx context.Context) UploadWaiter {
	if ctx == nil {
		return nil
	}
	if w, ok := ctx.Value(UploadWaiterKey).(UploadWaiter); ok {
		return w
	}
	return nil
}

// Action types for Control Plane Arrow Flight
const (
	ActionRegisterWorker     = "register-worker"
	ActionDeregisterWorker   = "deregister-worker"
	ActionRegisterClient     = "register-client"
	ActionDeregisterClient   = "deregister-client"
	ActionHeartbeat          = "heartbeat"
	ActionSubmitWorkflow     = "submit-workflow"
	ActionUpdateCapabilities = "update-capabilities"
	ActionPurgeWorkflow      = "purge-workflow"
	ActionPurgeAck           = "purge-ack"
	ActionGetWorkerInfo      = "get-worker-info"
	ActionRequestFile        = "request-file"
)

// RegistryInfo contains the metadata about all registered steps in the cluster.
type RegistryInfo struct {
	Steps     map[string]schema.StepSchemas     `json:"steps"`
	Resources map[string]schema.ResourceSchemas `json:"resources,omitempty"`
}

// Task Statuses
const (
	TaskStatusSuccess = "SUCCESS"
	TaskStatusFailed  = "FAILED"
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
	WorkflowID        string                               `json:"workflow_id"`
	TaskID            string                               `json:"task_id"`
	PreviousTaskID    string                               `json:"previous_task_id,omitempty"` // Deprecated: use PreviousTaskIDs
	PreviousTaskIDs   []string                            `json:"previous_task_ids,omitempty"`
	ParentAssignments map[string]string                   `json:"parent_assignments,omitempty"`
	Step              ir.StepInstruction                   `json:"step"`
	Resources         map[string]plugin.ResourceDefinition `json:"resources,omitempty"`
}

// WorkerHeartbeat is sent periodically by workers to the Control Plane.
type WorkerHeartbeat struct {
	Timestamp time.Time `json:"timestamp"`
	Load      int       `json:"load"` // Current number of active tasks
}

// Task represents a unit of work dispatched to a worker.
type Task struct {
	ID             string                        `json:"id"`
	ClientID       string                        `json:"client_id"`
	Program        ir.Program                    `json:"program"`
	TargetWorkflow string                        `json:"target_workflow,omitempty"`
	Strategy       string                        `json:"strategy"`
	Schemas        map[string]schema.StepSchemas `json:"schemas"`
}

// TaskResult is the response from a worker after executing a task.
type TaskResult struct {
	TaskID        string            `json:"task_id"`
	Status        string            `json:"status"`
	ErrorMessage  string            `json:"error_message,omitempty"`
	OutputHandles map[string]string `json:"output_handles,omitempty"`
}

// WorkflowSubmission contains the source code of a Heddle program to be compiled and executed.
type WorkflowSubmission struct {
	Source       string `json:"source"`
	WorkflowName string `json:"workflow_name,omitempty"`
	Strategy     string `json:"strategy"`
	Async        bool   `json:"async"`
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
	Type        string         `json:"type"`
	PurgeData   *WorkflowPurge `json:"purge,omitempty"`
	PurgeAck    *PurgeAck      `json:"purge_ack,omitempty"`
	LogData     *LogData       `json:"log_data,omitempty"`
	FileRequest *FileRequest   `json:"file_request,omitempty"`
}

// FileRequest is sent by a worker to request a file from the client.
type FileRequest struct {
	WorkflowID    string            `json:"workflow_id"`
	TaskID        string            `json:"task_id"`
	FilePath      string            `json:"file_path"`
	WorkerAddress string            `json:"worker_address"`
	Options       map[string]any    `json:"options,omitempty"`
	Columns       map[string]string `json:"columns,omitempty"`
}

// LogData contains text output from step execution.
type LogData struct {
	WorkflowID string `json:"workflow_id"`
	TaskID     string `json:"task_id"`
	Text       string `json:"text"`
}
