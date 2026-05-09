package execution

import (
	"time"
)

// WorkerStatus defines the current state of a worker.
type WorkerStatus string

const (
	WorkerStatusIdle     WorkerStatus = "idle"
	WorkerStatusBusy     WorkerStatus = "busy"
	WorkerStatusOffline  WorkerStatus = "offline"
	WorkerStatusDraining WorkerStatus = "draining"
)

// WorkerRegistration contains metadata sent by a worker when it first joins the cluster.
type WorkerRegistration struct {
	Address    string            `json:"address"`
	UDSAddress string            `json:"uds_address,omitempty"`
	Tags       map[string]string `json:"tags"`
	Runtime    string            `json:"runtime"` // e.g., "go", "python", "rust"
}

// Heartbeat is sent periodically by workers to the control plane.
type Heartbeat struct {
	Timestamp time.Time    `json:"timestamp"`
	Status    WorkerStatus `json:"status"`
	Load      float64      `json:"load"` // e.g., 0.0 to 1.0
}

// TaskUpdate reports the status of a specific instruction execution.
type TaskUpdate struct {
	TaskID       string    `json:"task_id"`
	Status       string    `json:"status"` // "running", "completed", "failed"
	Error        string    `json:"error,omitempty"`
	OutputHandle string    `json:"output_handle,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
}

// Action types for Arrow Flight
const (
	ActionRegisterWorker = "register-worker"
	ActionHeartbeat      = "heartbeat"
	ActionSubmitWorkflow = "submit-workflow"
)
