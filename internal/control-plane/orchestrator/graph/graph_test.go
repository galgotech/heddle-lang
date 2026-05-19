package graph_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"

	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator/graph"
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

func TestGraphOrchestrator_OrchestrateTask_Success(t *testing.T) {
	s := newMockServer()
	reg := registry.NewWorkerRegistry()
	workerID := "worker-1"
	reg.Register(workerID, models.WorkerRegistration{Address: "localhost:1234"})
	reg.UpdateCapabilities(workerID, models.WorkerCapabilitiesUpdate{Capabilities: []string{"std.print"}})
	o := graph.NewGraphOrchestrator(reg)

	stream := &mockStream{
		sentData: make(chan *flight.FlightData, 2),
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
					Next:            []string{"step-2"},
				},
				"step-2": &ir.StepInstruction{
					BaseInstruction: ir.BaseInstruction{ID: "step-2"},
					Call:            []string{"std", "print"},
				},
			},
			Workflows: []string{"flow-1"},
		},
	}

	// We simulate the worker completing both steps in another goroutine
	go func() {
		for i := 1; i <= 2; i++ {
			select {
			case <-stream.sentData:
				time.Sleep(50 * time.Millisecond)
				stepID := "step-1"
				if i == 2 {
					stepID = "step-2"
				}
				val, ok := s.pendingResults.Load(stepID)
				if ok {
					ch := val.(chan models.TaskResult)
					ch <- models.TaskResult{
						TaskID: stepID,
						Status: models.TaskStatusSuccess,
					}
				}
			case <-time.After(2 * time.Second):
			}
		}
	}()

	o.OrchestrateTask(context.Background(), task)

	assert.Equal(t, "task-1", s.signaledTaskID)
	assert.NoError(t, s.signaledErr)
}

func TestGraphOrchestrator_CycleDetection(t *testing.T) {
	s := newMockServer()
	reg := registry.NewWorkerRegistry()
	o := graph.NewGraphOrchestrator(reg)

	task := models.Task{
		ID: "task-cyclic",
		Program: &ir.Program{
			Instructions: map[string]any{
				"flow-1": &ir.FlowInstruction{
					BaseInstruction: ir.BaseInstruction{ID: "flow-1"},
					Heads:           []string{"step-1"},
				},
				"step-1": &ir.StepInstruction{
					BaseInstruction: ir.BaseInstruction{ID: "step-1"},
					Call:            []string{"std", "print"},
					Next:            []string{"step-2"},
				},
				"step-2": &ir.StepInstruction{
					BaseInstruction: ir.BaseInstruction{ID: "step-2"},
					Call:            []string{"std", "print"},
					Next:            []string{"step-1"}, // Circular dependency!
				},
			},
			Workflows: []string{"flow-1"},
		},
	}

	o.OrchestrateTask(context.Background(), task)

	assert.Equal(t, "task-cyclic", s.signaledTaskID)
	assert.Error(t, s.signaledErr)
	assert.Contains(t, s.signaledErr.Error(), "cycle detected")
}
