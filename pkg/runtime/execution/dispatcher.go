package execution

import (
	"fmt"
	"sync"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/sdk/go/proto"
)

// Task represents an instruction assigned to a worker.
type Task struct {
	ID           string              `json:"id"`
	Step         *ir.StepInstruction `json:"step"`
	InputHandle  string              `json:"input_handle,omitempty"`
	RemoteTicket *proto.FlightTicket `json:"remote_ticket,omitempty"`
}

// TaskStatus represents the lifecycle of a task.
type TaskStatus string

const (
	TaskStatusPending  TaskStatus = "pending"
	TaskStatusInFlight TaskStatus = "inflight"
	TaskStatusDone     TaskStatus = "completed"
	TaskStatusFailed   TaskStatus = "failed"
)

// TaskState tracks the execution details of a specific instruction.
type TaskState struct {
	Status       TaskStatus
	Error        string
	OutputHandle string
}

// Dispatcher manages the execution state of a Heddle program.
type Dispatcher struct {
	mu      sync.RWMutex
	program *ir.ProgramIR
	states  map[string]*TaskState
	results map[string]string // Maps Assignment name or Step ID to OutputHandle
	History []TaskUpdate      // Store history of states for Time-Travel Debugging
}

// NewDispatcher creates a new instance of the Dispatcher.
func NewDispatcher(program *ir.ProgramIR) *Dispatcher {
	return &Dispatcher{
		program: program,
		states:  make(map[string]*TaskState),
		results: make(map[string]string),
	}
}

// NextTasks returns the list of tasks that are ready to be executed.
func (d *Dispatcher) NextTasks() []Task {
	d.mu.Lock()
	defer d.mu.Unlock()

	var tasks []Task

	// 1. Check workflows (entry points)
	for _, flowID := range d.program.Workflows {
		flow := d.program.Instructions[flowID].(*ir.FlowInstruction)
		for _, headID := range flow.Heads {
			if d.isReady(headID) {
				tasks = append(tasks, d.createTask(headID, ""))
			}
		}
	}

	// 2. Check for follow-up tasks from completed ones
	for id, state := range d.states {
		if state.Status == TaskStatusDone {
			inst := d.program.Instructions[id].(*ir.StepInstruction)
			if inst.Next != "" && d.isReady(inst.Next) {
				// Pass the output of the current step as input to the next one
				tasks = append(tasks, d.createTask(inst.Next, state.OutputHandle))
			}
		} else if state.Status == TaskStatusFailed {
			inst := d.program.Instructions[id].(*ir.StepInstruction)
			if inst.Handler != "" && d.isReady(inst.Handler) {
				// Trigger handler
				raw := d.program.Instructions[inst.Handler]
				if flow, ok := raw.(*ir.FlowInstruction); ok {
					for _, headID := range flow.Heads {
						if d.isReady(headID) {
							tasks = append(tasks, d.createTask(headID, ""))
						}
					}
					// Mark the flow itself as in-flight
					d.states[inst.Handler] = &TaskState{Status: TaskStatusInFlight}
				} else {
					tasks = append(tasks, d.createTask(inst.Handler, ""))
				}
			}
		}
	}

	return tasks
}

func (d *Dispatcher) isReady(id string) bool {
	state, exists := d.states[id]
	if !exists {
		return true
	}
	return state.Status == TaskStatusPending
}

func (d *Dispatcher) createTask(id string, inputHandle string) Task {
	inst := d.program.Instructions[id].(*ir.StepInstruction)
	d.states[id] = &TaskState{Status: TaskStatusInFlight}
	return Task{
		ID:          id,
		Step:        inst,
		InputHandle: inputHandle,
	}
}

// ReportUpdate updates the state of a task based on worker feedback.
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

	// Record assignment if present
	inst := d.program.Instructions[update.TaskID].(*ir.StepInstruction)
	if inst.Assignment != "" && update.Status == string(TaskStatusDone) {
		d.results[inst.Assignment] = update.OutputHandle
	}

	// Record to history
	d.History = append(d.History, update)

	fmt.Printf("Dispatcher: Task %s updated to %s (Handle: %s)\n",
		update.TaskID, update.Status, update.OutputHandle)
}
