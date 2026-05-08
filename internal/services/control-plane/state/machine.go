package state

import (
	"context"
	"errors"
	"sync"
	"time"
)

// State defines the execution lifecycle phases of a DAG node.
type State int

const (
	// Pending indicates the node is initialized but not yet picked up by the scheduler.
	Pending State = iota
	// Running indicates the node is currently being executed by a worker.
	Running
	// WaitState indicates the node is parked, typically awaiting a timer or external signal.
	WaitState
	// Completed indicates the node has finished execution successfully.
	Completed
	// Failed indicates the node encountered an unrecoverable error during execution.
	Failed
)

// String returns the human-readable representation of the execution state.
func (s State) String() string {
	switch s {
	case Pending:
		return "Pending"
	case Running:
		return "Running"
	case WaitState:
		return "WaitState"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// Credentials holds the security context and authorization roles for an execution thread.
type Credentials struct {
	Token string
	Roles []string
}

// Lineage provides traceability for a node within the context of a specific DAG execution.
type Lineage struct {
	DAGID    string
	NodeID   string
	ParentID string
}

// Metadata encapsulates arbitrary execution-time values passed between nodes.
type Metadata struct {
	Values map[string]any `json:"values"`
}

// NodeSnapshot provides a point-in-time, serializable representation of a node's status for monitoring.
type NodeSnapshot struct {
	ID        string    `json:"task_id"`
	State     string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Error     string    `json:"error,omitempty"`
}

// HeddleContext extends the standard context with security, lineage, and metadata required for DAG orchestration.
type HeddleContext struct {
	context.Context
	Creds    Credentials
	Lineage  Lineage
	Metadata Metadata
}

// NewHeddleContext initializes a HeddleContext, ensuring metadata maps are instantiated to prevent nil access.
func NewHeddleContext(parent context.Context, creds Credentials, lineage Lineage, meta Metadata) *HeddleContext {
	if parent == nil {
		parent = context.Background()
	}
	if meta.Values == nil {
		meta.Values = make(map[string]any)
	}
	return &HeddleContext{
		Context:  parent,
		Creds:    creds,
		Lineage:  lineage,
		Metadata: meta,
	}
}

// Node represents the internal state and synchronization primitive for an atomic execution unit in the DAG.
type Node struct {
	id        string
	state     State
	createdAt time.Time
	updatedAt time.Time
	err       error
	mu        sync.RWMutex // Protects internal state during concurrent transitions and status reads.
}

// GetState returns the current execution state of the node.
func (n *Node) GetState() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// GetError returns the error that caused the node to enter the Failed state, if any.
func (n *Node) GetError() error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.err
}

// NewNode creates a new Node instance initialized to the Pending state.
func NewNode(id string) *Node {
	now := time.Now()
	return &Node{
		id:        id,
		state:     Pending,
		createdAt: now,
		updatedAt: now,
	}
}

// StateMachine defines the contract for managing lifecycle transitions across all nodes in a DAG.
type StateMachine interface {
	// AddNode registers a new node in the state machine index.
	AddNode(node *Node) error
	// GetNode retrieves a node pointer by its unique identifier.
	GetNode(id string) (*Node, error)
	// Transition attempts an atomic state change, validating the current state against the expected baseline.
	Transition(id string, expected State, next State, err error) error
	// GetHistory returns an immutable snapshot of all registered nodes for observability purposes.
	GetHistory() []NodeSnapshot
}

// DefaultStateMachine provides a thread-safe implementation of the StateMachine interface using an in-memory map.
type DefaultStateMachine struct {
	nodes map[string]*Node
	mu    sync.RWMutex // Protects the nodes map from concurrent registration and lookups.
}

// NewStateMachine returns a factory-initialized instance of DefaultStateMachine.
func NewStateMachine() StateMachine {
	return &DefaultStateMachine{
		nodes: make(map[string]*Node),
	}
}

// AddNode adds a node to the registry; returns an error if the node ID is already registered.
func (sm *DefaultStateMachine) AddNode(node *Node) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.nodes[node.id]; exists {
		return errors.New("node already exists")
	}
	sm.nodes[node.id] = node
	return nil
}

// GetNode looks up a node by ID; returns an error if the node is not found in the registry.
func (sm *DefaultStateMachine) GetNode(id string) (*Node, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	node, exists := sm.nodes[id]
	if !exists {
		return nil, errors.New("node not found")
	}
	return node, nil
}

// ErrInvalidTransition is returned when a requested state change does not match the node's current state.
var ErrInvalidTransition = errors.New("invalid state transition")

// Transition performs an atomic state update. It first verifies the node exists and then validates the state change.
func (sm *DefaultStateMachine) Transition(id string, expected State, next State, err error) error {
	// Identify the node within the registry.
	sm.mu.RLock()
	node, exists := sm.nodes[id]
	sm.mu.RUnlock()

	if !exists {
		return errors.New("node not found")
	}

	// Update the specific node state under its own lock to minimize contention.
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.state != expected {
		return ErrInvalidTransition
	}

	node.state = next
	node.updatedAt = time.Now()
	// Persistence of error context is only permitted if transitioning to the Failed state.
	if err != nil && next == Failed {
		node.err = err
	}

	return nil
}

// GetHistory iterates through the registry and produces a serializable status list.
func (sm *DefaultStateMachine) GetHistory() []NodeSnapshot {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	history := make([]NodeSnapshot, 0, len(sm.nodes))
	for _, node := range sm.nodes {
		node.mu.RLock()
		snapshot := NodeSnapshot{
			ID:        node.id,
			State:     node.state.String(),
			CreatedAt: node.createdAt,
			UpdatedAt: node.updatedAt,
		}
		if node.err != nil {
			snapshot.Error = node.err.Error()
		}
		node.mu.RUnlock()
		history = append(history, snapshot)
	}
	return history
}
