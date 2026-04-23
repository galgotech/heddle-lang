package execution

import (
	"fmt"
	"sync"

	"github.com/galgotech/heddle-lang/pkg/ir"
)

// Task represents an instruction assigned to a worker.
type Task struct {
	ID   string
	Step *ir.StepInstruction
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
	Status TaskStatus
	Error  string
}

// Dispatcher manages the execution state of a Heddle program.
type Dispatcher struct {
	mu      sync.RWMutex
	program *ir.ProgramIR
	states  map[string]*TaskState
}

// NewDispatcher creates a new instance of the Dispatcher.
func NewDispatcher(program *ir.ProgramIR) *Dispatcher {
	return &Dispatcher{
		program: program,
		states:  make(map[string]*TaskState),
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
				tasks = append(tasks, d.createTask(headID))
			}
		}
	}

	// 2. Check for follow-up tasks from completed ones
	for id, state := range d.states {
		if state.Status == TaskStatusDone {
			inst := d.program.Instructions[id].(*ir.StepInstruction)
			if inst.Next != "" && d.isReady(inst.Next) {
				tasks = append(tasks, d.createTask(inst.Next))
			}
		} else if state.Status == TaskStatusFailed {
			inst := d.program.Instructions[id].(*ir.StepInstruction)
			if inst.Handler != "" && d.isReady(inst.Handler) {
				tasks = append(tasks, d.createTask(inst.Handler))
			}
		}
	}

	return tasks
}

func (d *Dispatcher) isReady(id string) bool {
	state, exists := d.states[id]
	if !exists {
		return true // Never seen before, so it's ready if its dependencies are met
	}
	return state.Status == TaskStatusPending
}

func (d *Dispatcher) createTask(id string) Task {
	inst := d.program.Instructions[id].(*ir.StepInstruction)
	d.states[id] = &TaskState{Status: TaskStatusInFlight}
	return Task{
		ID:   id,
		Step: inst,
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

	fmt.Printf("Dispatcher: Task %s updated to %s\n", update.TaskID, update.Status)
}
