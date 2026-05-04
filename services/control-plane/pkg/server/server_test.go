package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
	"github.com/galgotech/heddle-lang/services/control-plane/pkg/state"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

// mockDoActionServer implements flight.FlightService_DoActionServer for testing
type mockDoActionServer struct {
	flight.FlightService_DoActionServer
	results []*flight.Result
}

func (m *mockDoActionServer) Send(r *flight.Result) error {
	m.results = append(m.results, r)
	return nil
}

func (m *mockDoActionServer) Context() context.Context {
	return context.Background()
}

func (m *mockDoActionServer) SetHeader(metadata.MD) error  { return nil }
func (m *mockDoActionServer) SendHeader(metadata.MD) error { return nil }
func (m *mockDoActionServer) SetTrailer(metadata.MD)       {}

func TestControlPlaneServer_DoAction_RegisterWorker(t *testing.T) {
	server := NewControlPlaneServer()
	mockStream := &mockDoActionServer{}

	reg := execution.WorkerRegistration{
		WorkerID: "worker-1",
		Address:  "localhost:50052",
		Runtime:  "go",
	}
	body, _ := json.Marshal(reg)

	action := &flight.Action{
		Type: execution.ActionRegisterWorker,
		Body: body,
	}

	err := server.DoAction(action, mockStream)
	assert.NoError(t, err)
	assert.Len(t, mockStream.results, 1)
	assert.Equal(t, "OK", string(mockStream.results[0].Body))

	worker, err := server.registry.GetHealthyWorker()
	assert.NoError(t, err)
	assert.Equal(t, "worker-1", worker.ID)
}

func TestControlPlaneServer_DoAction_Heartbeat(t *testing.T) {
	server := NewControlPlaneServer()
	mockStream := &mockDoActionServer{}

	// Register first
	server.registry.Register("worker-1", "localhost:50051", "", nil)

	hb := execution.Heartbeat{
		WorkerID:  "worker-1",
		Timestamp: time.Now(),
		Status:    execution.WorkerStatusIdle,
		Load:      0.1,
	}
	body, _ := json.Marshal(hb)

	action := &flight.Action{
		Type: execution.ActionHeartbeat,
		Body: body,
	}

	err := server.DoAction(action, mockStream)
	assert.NoError(t, err)
	assert.Len(t, mockStream.results, 1)
	assert.Equal(t, "OK", string(mockStream.results[0].Body))

	worker, err := server.registry.GetHealthyWorker()
	assert.NoError(t, err)
	assert.Equal(t, "worker-1", worker.ID)
}

func TestControlPlaneServer_DoAction_SubmitWorkflow(t *testing.T) {
	server := NewControlPlaneServer()
	mockStream := &mockDoActionServer{}

	source := `
step s1: void -> void = m.s1
workflow main {
  s1
}
`
	action := &flight.Action{
		Type: execution.ActionSubmitWorkflow,
		Body: []byte(source),
	}

	err := server.DoAction(action, mockStream)
	assert.NoError(t, err)
	assert.Len(t, mockStream.results, 1)
	assert.Equal(t, "Workflow initialized successfully", string(mockStream.results[0].Body))

	server.mu.RLock()
	defer server.mu.RUnlock()
	assert.NotNil(t, server.dispatcher)
}

func TestControlPlaneServer_DoAction_GetHistory(t *testing.T) {
	server := NewControlPlaneServer()
	mockStream := &mockDoActionServer{}

	// Add a node to state machine
	node := state.NewNode("task-1")
	// Using the internal node directly for simplicity in testing
	server.sm.AddNode(node)
	server.sm.Transition("task-1", state.Pending, state.Completed, nil)

	action := &flight.Action{
		Type: execution.ActionGetHistory,
	}

	err := server.DoAction(action, mockStream)
	assert.NoError(t, err)
	assert.Len(t, mockStream.results, 1)

	var history []state.NodeSnapshot
	err = json.Unmarshal(mockStream.results[0].Body, &history)
	assert.NoError(t, err)
	assert.Len(t, history, 1)
	assert.Equal(t, "task-1", history[0].ID)
	assert.Equal(t, "Completed", history[0].State)
}
