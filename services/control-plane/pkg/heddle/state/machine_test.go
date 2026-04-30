package state

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateString(t *testing.T) {
	assert.Equal(t, "Pending", Pending.String())
	assert.Equal(t, "Running", Running.String())
	assert.Equal(t, "WaitState", WaitState.String())
	assert.Equal(t, "Completed", Completed.String())
	assert.Equal(t, "Failed", Failed.String())
	assert.Equal(t, "Unknown", State(99).String())
}

func TestHeddleContext(t *testing.T) {
	parent := context.Background()
	creds := Credentials{Token: "test-token", Roles: []string{"admin", "user"}}
	lineage := Lineage{DAGID: "dag-1", NodeID: "node-1", ParentID: "root"}
	meta := Metadata{Values: map[string]interface{}{"retry_count": 3}}

	ctx := NewHeddleContext(parent, creds, lineage, meta)

	assert.NotNil(t, ctx)
	assert.Equal(t, "test-token", ctx.Creds.Token)
	assert.Equal(t, []string{"admin", "user"}, ctx.Creds.Roles)
	assert.Equal(t, "dag-1", ctx.Lineage.DAGID)
	assert.Equal(t, "node-1", ctx.Lineage.NodeID)
	assert.Equal(t, "root", ctx.Lineage.ParentID)
	assert.Equal(t, 3, ctx.Metadata.Values["retry_count"])

	// Test propagation
	valCtx := context.WithValue(ctx, "key", "value")
	assert.Equal(t, "value", valCtx.Value("key"))

	// Test nil parent and nil metadata values
	emptyCtx := NewHeddleContext(nil, creds, lineage, Metadata{})
	assert.NotNil(t, emptyCtx.Context) // Should default to context.Background()
	assert.NotNil(t, emptyCtx.Metadata.Values)
}

func TestStateMachine_AddNode(t *testing.T) {
	sm := NewStateMachine()
	node := NewNode("node-1")

	err := sm.AddNode(node)
	assert.NoError(t, err)

	// Attempt to add the same node again
	err = sm.AddNode(node)
	assert.Error(t, err)
	assert.Equal(t, "node already exists", err.Error())
}

func TestStateMachine_GetNode(t *testing.T) {
	sm := NewStateMachine()
	node := NewNode("node-1")
	sm.AddNode(node)

	retrieved, err := sm.GetNode("node-1")
	assert.NoError(t, err)
	assert.Equal(t, node, retrieved)

	_, err = sm.GetNode("node-2")
	assert.Error(t, err)
	assert.Equal(t, "node not found", err.Error())
}

func TestStateMachine_Transition(t *testing.T) {
	sm := NewStateMachine()
	node := NewNode("node-1")
	sm.AddNode(node)

	// Valid transition: Pending -> Running
	err := sm.Transition("node-1", Pending, Running, nil)
	assert.NoError(t, err)

	retrieved, _ := sm.GetNode("node-1")
	assert.Equal(t, Running, retrieved.State)

	// Invalid transition: Pending -> Completed (Expected state was Running, not Pending)
	err = sm.Transition("node-1", Pending, Completed, nil)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidTransition, err)

	// Valid transition: Running -> Completed
	err = sm.Transition("node-1", Running, Completed, nil)
	assert.NoError(t, err)

	retrieved, _ = sm.GetNode("node-1")
	assert.Equal(t, Completed, retrieved.State)

	// Transition on non-existent node
	err = sm.Transition("non-existent", Pending, Running, nil)
	assert.Error(t, err)
	assert.Equal(t, "node not found", err.Error())

	// Transition to Failed with error
	node2 := NewNode("node-2")
	sm.AddNode(node2)
	testErr := errors.New("something went wrong")

	err = sm.Transition("node-2", Pending, Failed, testErr)
	assert.NoError(t, err)

	retrieved2, _ := sm.GetNode("node-2")
	assert.Equal(t, Failed, retrieved2.State)
	assert.Equal(t, testErr, retrieved2.Error)
}

func TestStateMachine_ConcurrentTransitions(t *testing.T) {
	sm := NewStateMachine()
	node := NewNode("node-1")
	sm.AddNode(node)

	var wg sync.WaitGroup
	wg.Add(2)

	// Try to transition Pending -> Running concurrently
	// Only one should succeed
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			err := sm.Transition("node-1", Pending, Running, nil)
			if err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, 1, successCount, "Only one concurrent transition should succeed")

	retrieved, _ := sm.GetNode("node-1")
	assert.Equal(t, Running, retrieved.State)
}
