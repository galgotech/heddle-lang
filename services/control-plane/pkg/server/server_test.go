package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
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

	server.mu.RLock()
	defer server.mu.RUnlock()
	assert.Contains(t, server.workers, "worker-1")
	assert.Equal(t, "go", server.workers["worker-1"].Runtime)
}

func TestControlPlaneServer_DoAction_Heartbeat(t *testing.T) {
	server := NewControlPlaneServer()
	mockStream := &mockDoActionServer{}

	// Register first
	reg := execution.WorkerRegistration{WorkerID: "worker-1"}
	server.workers["worker-1"] = reg

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

	server.mu.RLock()
	defer server.mu.RUnlock()
	assert.Contains(t, server.heartbeats, "worker-1")
	assert.Equal(t, execution.WorkerStatusIdle, server.heartbeats["worker-1"].Status)
}
