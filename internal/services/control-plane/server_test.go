package controlplane

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
	"github.com/galgotech/heddle-lang/pkg/runtime/state"
)

// mockDoActionServer is a mock implementation of flight.FlightService_DoActionServer
type mockDoActionServer struct {
	mock.Mock
	grpc.ServerStream
	ctx context.Context
}

func (m *mockDoActionServer) Send(res *flight.Result) error {
	args := m.Called(res)
	return args.Error(0)
}

func (m *mockDoActionServer) Context() context.Context {
	return m.ctx
}

func setupTestServer() (*FlightServer, *ControlPlane) {
	wr := NewWorkerRegistry()
	wq := NewWorkQueue()
	dl := NewDataLocalityRegistry()
	cp := NewControlPlane(wr, wq, dl)
	cp.Start()
	return NewFlightServer(cp), cp
}

func createHeddleContext(workerID string) context.Context {
	meta := state.Metadata{Values: map[string]any{"worker_id": workerID}}
	return state.NewHeddleContext(context.Background(), state.Credentials{}, meta)
}

func TestFlightServer_DoAction(t *testing.T) {
	server, controlPlane := setupTestServer()
	defer controlPlane.Stop()

	workerID := "test-worker"
	ctx := createHeddleContext(workerID)

	t.Run("RegisterWorker", func(t *testing.T) {
		reg := execution.WorkerRegistration{
			Address: "localhost:50051",
		}
		body, _ := json.Marshal(reg)
		action := &flight.Action{
			Type: execution.ActionRegisterWorker,
			Body: body,
		}

		mockStream := &mockDoActionServer{ctx: ctx}
		mockStream.On("Send", mock.MatchedBy(func(res *flight.Result) bool {
			return string(res.Body) == "OK"
		})).Return(nil)

		err := server.DoAction(action, mockStream)
		assert.NoError(t, err)
		mockStream.AssertExpectations(t)

		// Verify registration with retry
		var w *Worker
		var regErr error
		for range 20 {
			w, regErr = controlPlane.workerRegistry.GetWorker(workerID)
			if regErr == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		assert.NoError(t, regErr)
		if w != nil {
			assert.Equal(t, workerID, w.ID)
		} else {
			t.Fatal("worker should not be nil")
		}
	})

	t.Run("Heartbeat", func(t *testing.T) {
		hb := execution.Heartbeat{
			Status: execution.WorkerStatusIdle,
		}
		body, _ := json.Marshal(hb)
		action := &flight.Action{
			Type: execution.ActionHeartbeat,
			Body: body,
		}

		mockStream := &mockDoActionServer{ctx: ctx}
		mockStream.On("Send", mock.MatchedBy(func(res *flight.Result) bool {
			return string(res.Body) == "OK"
		})).Return(nil)

		err := server.DoAction(action, mockStream)
		assert.NoError(t, err)
		mockStream.AssertExpectations(t)
	})

	t.Run("SubmitWorkflow", func(t *testing.T) {
		source := `
import "std/io" io

workflow main {
  []
    | io.print
}
`
		action := &flight.Action{
			Type: execution.ActionSubmitWorkflow,
			Body: []byte(source),
		}

		mockStream := &mockDoActionServer{ctx: ctx}
		mockStream.On("Send", mock.MatchedBy(func(res *flight.Result) bool {
			return string(res.Body) == "Workflow initialized successfully"
		})).Return(nil)

		err := server.DoAction(action, mockStream)
		assert.NoError(t, err)
		mockStream.AssertExpectations(t)
	})

	t.Run("UnknownAction", func(t *testing.T) {
		action := &flight.Action{
			Type: "unknown",
		}
		mockStream := &mockDoActionServer{ctx: ctx}
		err := server.DoAction(action, mockStream)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown action")
	})

	t.Run("InvalidRegistration", func(t *testing.T) {
		action := &flight.Action{
			Type: execution.ActionRegisterWorker,
			Body: []byte("invalid json"),
		}
		mockStream := &mockDoActionServer{ctx: ctx}
		err := server.DoAction(action, mockStream)
		assert.Error(t, err)
	})
}
