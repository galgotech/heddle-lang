package manager

import (
	"errors"
	"sync"
	"time"
)

// WorkerState represents the availability and health status of an execution node.
type WorkerState string

const (
	// WorkerHealthy indicates the node is responsive and ready to accept task delegations.
	WorkerHealthy WorkerState = "Healthy"
	// WorkerDegraded indicates the node is reachable but operating under resource pressure or sub-optimal conditions.
	WorkerDegraded WorkerState = "Degraded" // TODO: currently is not been used
	// WorkerOffline indicates the node is unreachable or has missed its heartbeat expiration window.
	WorkerOffline WorkerState = "Offline" // TODO: currently is not been used
)

// Worker represents a stateless execution unit within the Heddle cluster.
// In the Host-Core Symbiosis model, workers receive JIT-compiled instructions
// from the Control Plane to perform transformations on Arrow-native data.
type Worker struct {
	ID         string            // ID is the unique identity of the worker node.
	Address    string            // Address is the TCP/gRPC endpoint for remote service delegation.
	UDSAddress string            // UDSAddress is the Unix Domain Socket path for zero-copy communication on local hosts.
	Labels     map[string]string // Labels provide capability metadata (e.g., GPU, region) for requirement-based scheduling.
	State      WorkerState       // State tracks the current operational health of the worker.
	LastSeenAt time.Time         // LastSeenAt records the timestamp of the most recent heartbeat or registration event.
}

// WorkerRegistry defines the interface for managing the discovery and liveness of execution nodes.
// Implementations must ensure thread-safe access to the worker population.
type WorkerRegistry interface {
	// Register adds a new node or updates an existing one in the active worker pool.
	Register(id string, address string, udsAddress string, labels map[string]string)
	// GetWorker retrieves metadata for a specific worker by its identity.
	GetWorker(id string) (*Worker, error)
	// Heartbeat updates the liveness record for a worker, resetting its expiration window.
	Heartbeat(id string) error
	// GetHealthyWorker selects an available, healthy node for task assignment.
	GetHealthyWorker() (*Worker, error)
}

// DefaultWorkerRegistry provides an in-memory, thread-safe implementation of the WorkerRegistry interface.
type DefaultWorkerRegistry struct {
	mu      sync.RWMutex
	workers map[string]*Worker
}

// Register performs an atomic upsert of a worker node, initializing it in a healthy state.
func (r *DefaultWorkerRegistry) Register(id string, address string, udsAddress string, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[id] = &Worker{
		ID:         id,
		Address:    address,
		UDSAddress: udsAddress,
		Labels:     labels,
		State:      WorkerHealthy,
		LastSeenAt: time.Now(),
	}
}

// GetWorker returns the worker metadata associated with the provided ID, returning an error if not found.
func (r *DefaultWorkerRegistry) GetWorker(id string) (*Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	worker, exists := r.workers[id]
	if !exists {
		return nil, errors.New("worker not found")
	}
	return worker, nil
}

// Heartbeat marks a worker as healthy and refreshes its liveness timestamp to prevent expiration.
func (r *DefaultWorkerRegistry) Heartbeat(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	worker, exists := r.workers[id]
	if !exists {
		return errors.New("worker not found")
	}

	worker.LastSeenAt = time.Now()
	worker.State = WorkerHealthy
	return nil
}

// GetHealthyWorker scans the registry for a node that is both in a Healthy state and has
// provided a heartbeat within the strict 30-second liveness threshold.
func (r *DefaultWorkerRegistry) GetHealthyWorker() (*Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, w := range r.workers {
		// Nodes that haven't checked in within the threshold are skipped to prevent dispatching to dead nodes.
		if time.Since(w.LastSeenAt) > 30*time.Second {
			continue
		}
		if w.State == WorkerHealthy {
			return w, nil
		}
	}

	return nil, errors.New("no healthy workers available")
}

// NewRegistry constructs a fresh instance of the default worker registry.
func NewRegistry() WorkerRegistry {
	return &DefaultWorkerRegistry{
		workers: make(map[string]*Worker),
	}
}
