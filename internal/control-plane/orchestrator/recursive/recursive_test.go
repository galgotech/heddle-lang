package recursive_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"

	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator/recursive"
	"github.com/galgotech/heddle-lang/internal/control-plane/registry"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
)

type mockWorker struct {
	id string
}

func (m *mockWorker) GetID() string {
	return m.id
}

type mockServer struct {
	pendingResults sync.Map
	purged         map[string]bool
	signaledErr    error
	signaledTaskID string
	mu             sync.Mutex
}

func newMockServer() *mockServer {
	return &mockServer{
		purged: make(map[string]bool),
	}
}

func (m *mockServer) PurgeWorkflow(ctx context.Context, workflowID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.purged[workflowID] = true
}

type mockStream struct {
	flight.FlightService_DoExchangeServer
	sentData chan *flight.FlightData
}

func (m *mockStream) Send(data *flight.FlightData) error {
	m.sentData <- data
	return nil
}

func TestRecursiveOrchestrator_OrchestrateTask_NotImplemented(t *testing.T) {
	s := newMockServer()
	reg := registry.NewWorkerRegistry()
	o := recursive.NewRecursiveOrchestrator(reg)

	task := models.Task{
		ID: "task-1",
		Program: &ir.Program{
			Instructions: map[string]any{
				"flow-1": &ir.FlowInstruction{
					BaseInstruction: ir.BaseInstruction{ID: "flow-1"},
					Heads:           []string{"step-1"},
				},
				"step-1": &ir.StepInstruction{
					BaseInstruction: ir.BaseInstruction{ID: "step-1"},
					Call:            []string{"std", "print"},
				},
			},
			Workflows: []string{"flow-1"},
		},
	}

	o.OrchestrateTask(context.Background(), task)

	assert.Equal(t, "task-1", s.signaledTaskID)
	assert.Error(t, s.signaledErr)
	assert.Contains(t, s.signaledErr.Error(), "no worker found for capability: std.print")
}

func TestRecursiveOrchestrator_OrchestrateTask_Success(t *testing.T) {
	s := newMockServer()
	reg := registry.NewWorkerRegistry()
	workerID := "worker-1"
	reg.Register(workerID, models.WorkerRegistration{Address: "localhost:1234"})
	reg.UpdateCapabilities(workerID, models.WorkerCapabilitiesUpdate{Capabilities: []string{"std.print"}})
	o := recursive.NewRecursiveOrchestrator(reg)

	stream := &mockStream{
		sentData: make(chan *flight.FlightData, 1),
	}
	reg.ProcessStream(workerID, stream)

	task := models.Task{
		ID: "task-1",
		Program: &ir.Program{
			Instructions: map[string]any{
				"flow-1": &ir.FlowInstruction{
					BaseInstruction: ir.BaseInstruction{ID: "flow-1"},
					Heads:           []string{"step-1"},
				},
				"step-1": &ir.StepInstruction{
					BaseInstruction: ir.BaseInstruction{ID: "step-1"},
					Call:            []string{"std", "print"},
				},
			},
			Workflows: []string{"flow-1"},
		},
	}

	// We simulate the worker completing the task in another goroutine
	go func() {
		select {
		case <-stream.sentData:
			// Let the orchestrator store the result channel
			time.Sleep(50 * time.Millisecond)
			val, ok := s.pendingResults.Load("step-1")
			if ok {
				ch := val.(chan models.TaskResult)
				ch <- models.TaskResult{
					TaskID: "step-1",
					Status: models.TaskStatusSuccess,
				}
			}
		case <-time.After(2 * time.Second):
		}
	}()

	o.OrchestrateTask(context.Background(), task)

	assert.Equal(t, "task-1", s.signaledTaskID)
	assert.NoError(t, s.signaledErr)
}
