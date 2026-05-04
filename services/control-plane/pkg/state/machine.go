package state

import (
	"context"
	"errors"
	"sync"
	"time"
)

// State represents the execution state of a DAG node.
type State int

const (
	Pending State = iota
	Running
	WaitState
	Completed
	Failed
)

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

// Credentials represent security credentials.
type Credentials struct {
	Token string
	Roles []string
}

// Lineage tracks the execution history and origin in the DAG.
type Lineage struct {
	DAGID    string
	NodeID   string
	ParentID string
}

// Metadata contains execution metadata.
type Metadata struct {
	Values map[string]interface{} `json:"values"`
}

// NodeSnapshot represents a serializable view of a node's state.
type NodeSnapshot struct {
	ID        string    `json:"id"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Error     string    `json:"error,omitempty"`
}

// HeddleContext is a robust Context wrapper carrying security credentials, execution metadata, and DAG lineage.
type HeddleContext struct {
	context.Context
	Creds    Credentials
	Lineage  Lineage
	Metadata Metadata
}

// NewHeddleContext creates a new HeddleContext.
func NewHeddleContext(parent context.Context, creds Credentials, lineage Lineage, meta Metadata) *HeddleContext {
	if parent == nil {
		parent = context.Background()
	}
	if meta.Values == nil {
		meta.Values = make(map[string]interface{})
	}
	return &HeddleContext{
		Context:  parent,
		Creds:    creds,
		Lineage:  lineage,
		Metadata: meta,
	}
}

// Node represents a node in the DAG.
type Node struct {
	ID        string
	State     State
	CreatedAt time.Time
	UpdatedAt time.Time
	Error     error
	mu        sync.RWMutex
}

// NewNode creates a new DAG node.
func NewNode(id string) *Node {
	now := time.Now()
	return &Node{
		ID:        id,
		State:     Pending,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// StateMachine manages the state transitions for nodes in the DAG.
type StateMachine struct {
	nodes map[string]*Node
	mu    sync.RWMutex
}

// NewStateMachine creates a new state machine.
func NewStateMachine() *StateMachine {
	return &StateMachine{
		nodes: make(map[string]*Node),
	}
}

// AddNode adds a new node to the state machine.
func (sm *StateMachine) AddNode(node *Node) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.nodes[node.ID]; exists {
		return errors.New("node already exists")
	}
	sm.nodes[node.ID] = node
	return nil
}

// GetNode retrieves a node by its ID.
func (sm *StateMachine) GetNode(id string) (*Node, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	node, exists := sm.nodes[id]
	if !exists {
		return nil, errors.New("node not found")
	}
	return node, nil
}

var ErrInvalidTransition = errors.New("invalid state transition")

// Transition atomically transitions a node from an expected state to a new state.
func (sm *StateMachine) Transition(id string, expected State, next State, err error) error {
	sm.mu.RLock()
	node, exists := sm.nodes[id]
	sm.mu.RUnlock()

	if !exists {
		return errors.New("node not found")
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.State != expected {
		return ErrInvalidTransition
	}

	node.State = next
	node.UpdatedAt = time.Now()
	if err != nil && next == Failed {
		node.Error = err
	}

	return nil
}

// GetHistory returns a snapshot of all nodes in the state machine.
func (sm *StateMachine) GetHistory() []NodeSnapshot {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	history := make([]NodeSnapshot, 0, len(sm.nodes))
	for _, node := range sm.nodes {
		node.mu.RLock()
		snapshot := NodeSnapshot{
			ID:        node.ID,
			State:     node.State.String(),
			CreatedAt: node.CreatedAt,
			UpdatedAt: node.UpdatedAt,
		}
		if node.Error != nil {
			snapshot.Error = node.Error.Error()
		}
		node.mu.RUnlock()
		history = append(history, snapshot)
	}
	return history
}
