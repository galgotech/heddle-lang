package execution

import (
	"time"

	"github.com/galgotech/heddle-lang/pkg/ir"
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
	WorkerID string            `json:"worker_id"`
	Address  string            `json:"address"`
	Tags     map[string]string `json:"tags"`
	Runtime  string            `json:"runtime"` // e.g., "go", "python", "rust"
}

// Heartbeat is sent periodically by workers to the control plane.
type Heartbeat struct {
	WorkerID  string       `json:"worker_id"`
	Timestamp time.Time    `json:"timestamp"`
	Status    WorkerStatus `json:"status"`
	Load      float64      `json:"load"` // e.g., 0.0 to 1.0
}

// WarmupRequest tells a worker to pre-load specific steps.
type WarmupRequest struct {
	Steps []ir.StepInstruction `json:"steps"`
}

// TaskUpdate reports the status of a specific instruction execution.
type TaskUpdate struct {
	TaskID    string    `json:"task_id"`
	Status    string    `json:"status"` // "running", "completed", "failed"
	Error     string    `json:"error,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// Action types for Arrow Flight
const (
	ActionRegisterWorker = "register-worker"
	ActionHeartbeat      = "heartbeat"
	ActionWarmup         = "warmup"
)
