package execution

import (
	"fmt"
	"sync"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/sdk/go/proto"
)

// Task represents a specific unit of work assigned to a worker node.
// It encapsulates the IR instruction and the necessary Apache Arrow Flight tickets
// required for zero-copy data access.
type Task struct {
	ID      string                         `json:"id"`
	Step    *ir.StepInstruction            `json:"step"`
	Tickets map[string]*proto.FlightTicket `json:"tickets,omitempty"`
}

// TaskStatus defines the discrete states in a task's execution lifecycle.
type TaskStatus string

const (
	// TaskStatusPending indicates the task is waiting for its dependencies to resolve.
	TaskStatusPending TaskStatus = "pending"
	// TaskStatusInFlight indicates the task has been dispatched to a worker and is executing.
	TaskStatusInFlight TaskStatus = "inflight"
	// TaskStatusDone indicates the task completed successfully and produced an output handle.
	TaskStatusDone TaskStatus = "completed"
	// TaskStatusFailed indicates the task execution resulted in an error.
	TaskStatusFailed TaskStatus = "failed"
)

// TaskState maintains the runtime execution metadata for a specific IR instruction.
type TaskState struct {
	Status       TaskStatus
	Error        string
	OutputHandle string // Reference to the Apache Arrow record batch in the DataLocalityRegistry
}

// Dispatcher acts as the Smart Control Plane, managing the global execution state
// of a Heddle program by orchestrating task transitions across the DAG topology.
type Dispatcher struct {
	mu      sync.RWMutex
	program *ir.Program
	states  map[string]*TaskState
	results map[string]string // Maps Assignment labels or Step IDs to their respective OutputHandles
	History []TaskUpdate      // Immutable audit log for state machine transitions and Time-Travel Debugging
}

// NextTasks evaluates the current program state against the DAG topology to identify
// instructions that are ready for execution. It returns a batch of tasks to be dispatched.
func (d *Dispatcher) NextTasks() []Task {
	d.mu.Lock()
	defer d.mu.Unlock()

	var tasks []Task

	// 1. Evaluate workflow entry points (Heads) defined in the IR.
	for _, flowID := range d.program.Workflows {
		flow := d.program.Instructions[flowID].(*ir.FlowInstruction)
		for _, headID := range flow.Heads {
			if d.isReady(headID) {
				tasks = append(tasks, d.createTask(headID))
			}
		}
	}

	// 2. Traverse the DAG to identify follow-up tasks from recently completed or failed steps.
	for id, state := range d.states {
		if state.Status == TaskStatusDone {
			// Successful completion triggers the next nodes in the DAG.
			inst, ok := d.program.Instructions[id].(*ir.StepInstruction)
			if !ok {
				continue
			}
			for _, nextID := range inst.Next {
				if d.isReady(nextID) {
					tasks = append(tasks, d.createTask(nextID))
				}
			}
		} else if state.Status == TaskStatusFailed {
			// Failure triggers recovery logic if an error handler (Handler) is defined.
			inst, ok := d.program.Instructions[id].(*ir.StepInstruction)
			if !ok {
				continue
			}

			handlerID := inst.Handler
			if handlerID == "" {
				// Fallback to global workflow trap if step-level handler is missing.
				for _, flowID := range d.program.Workflows {
					if flow, ok := d.program.Instructions[flowID].(*ir.FlowInstruction); ok && flow.Handler != "" {
						handlerID = flow.Handler
						break
					}
				}
			}

			if handlerID != "" && d.isReady(handlerID) {
				raw := d.program.Instructions[handlerID]
				if flow, ok := raw.(*ir.FlowInstruction); ok {
					// If the handler is a Flow, dispatch all its entry points with the failure context.
					for _, headID := range flow.Heads {
						if d.isReady(headID) {
							tasks = append(tasks, d.createTask(headID))
						}
					}
					d.states[handlerID] = &TaskState{Status: TaskStatusInFlight}
				} else {
					// Dispatch the single recovery step.
					tasks = append(tasks, d.createTask(handlerID))
				}
			}
		}
	}

	return tasks
}

// isReady determines if an instruction is eligible for execution based on its current state.
func (d *Dispatcher) isReady(id string) bool {
	state, exists := d.states[id]
	if exists && state.Status != TaskStatusPending {
		return false // Already in-flight or completed
	}

	// An instruction is ready only if all its parents have completed successfully.
	inst, ok := d.program.Instructions[id].(*ir.StepInstruction)
	if !ok {
		return true // Flow instructions or others don't have parents in the same way
	}

	for _, parentID := range inst.Parents {
		pState, pExists := d.states[parentID]
		if !pExists || pState.Status != TaskStatusDone {
			return false
		}
	}

	return true
}

// createTask initializes a new Task object for an IR instruction, configuring
// Apache Arrow Flight tickets for data locality.
func (d *Dispatcher) createTask(id string) Task {
	inst := d.program.Instructions[id].(*ir.StepInstruction)
	d.states[id] = &TaskState{Status: TaskStatusInFlight}

	tickets := make(map[string]*proto.FlightTicket)

	// Resolve data locality for all incoming dependencies.
	for _, parentID := range inst.Parents {
		pState, exists := d.states[parentID]
		if !exists || pState.OutputHandle == "" {
			continue
		}

		pInst := d.program.Instructions[parentID].(*ir.StepInstruction)
		key := pInst.Assignment
		if key == "" {
			key = parentID
		}

		tickets[key] = &proto.FlightTicket{
			ResourceId: pState.OutputHandle,
			RouteType:  proto.RouteType_LOCAL,
		}
	}

	return Task{
		ID:      id,
		Step:    inst,
		Tickets: tickets,
	}
}

// ReportUpdate processes execution feedback from workers, transitioning task states
// and updating the global result registry for downstream dependency resolution.
func (d *Dispatcher) ReportUpdate(update TaskUpdate) {
	d.mu.Lock()
	defer d.mu.Unlock()

	state, exists := d.states[update.TaskID]
	if !exists {
		state = &TaskState{}
		d.states[update.TaskID] = state
	}

	state.Status = TaskStatus(update.Status)
	state.Error = update.Error
	state.OutputHandle = update.OutputHandle

	// If the instruction produces an assignment (e.g., 'x = step()'), map the label
	// to the physical memory handle for zero-copy lookups.
	inst := d.program.Instructions[update.TaskID].(*ir.StepInstruction)
	if inst.Assignment != "" && update.Status == string(TaskStatusDone) {
		d.results[inst.Assignment] = update.OutputHandle
	}

	// Append to history to facilitate execution visualization and time-travel debugging.
	d.History = append(d.History, update)

	fmt.Printf("Dispatcher: Task %s updated to %s (Handle: %s)\n",
		update.TaskID, update.Status, update.OutputHandle)
}

// NewDispatcher initializes a new Smart Control Plane instance for the given compiled program.
func NewDispatcher(program *ir.Program) *Dispatcher {
	return &Dispatcher{
		program: program,
		states:  make(map[string]*TaskState),
		results: make(map[string]string),
	}
}
