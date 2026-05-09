package execution

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
)

func TestDispatcher_BasicFlow(t *testing.T) {
	code := `
import "std/m" m

step s1 = m.extract
step s2 = m.transform

workflow main {
  s1
    | s2
}
`
	c := compiler.New()
	program, err := c.Compile(code)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	d := NewDispatcher(program)

	// Initial tasks (heads of the workflow)
	tasks := d.NextTasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 initial task, got %d", len(tasks))
	}

	task := tasks[0]
	if task.Step.DefinitionName != "s1" {
		t.Errorf("expected task s1, got %s", task.Step.DefinitionName)
	}

	// Complete s1
	d.ReportUpdate(TaskUpdate{
		TaskID: task.ID,
		Status: "completed",
	})

	// Should now have s2
	nextTasks := d.NextTasks()
	if len(nextTasks) != 1 {
		t.Fatalf("expected 1 next task, got %d", len(nextTasks))
	}

	nextTask := nextTasks[0]
	if nextTask.Step.DefinitionName != "s2" {
		t.Errorf("expected task s2, got %s", nextTask.Step.DefinitionName)
	}

	// Complete s2
	d.ReportUpdate(TaskUpdate{
		TaskID: nextTask.ID,
		Status: "completed",
	})

	// Should have no more tasks
	if len(d.NextTasks()) != 0 {
		t.Error("expected no more tasks")
	}
}

func TestDispatcher_WithHandlers(t *testing.T) {
	code := `
import "std/m" m

step s1 = m.extract
step r1 = m.retry

handler recover {
  *
    | r1
}

workflow main {
    s1 ? recover
}
`
	c := compiler.New()
	program, err := c.Compile(code)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	d := NewDispatcher(program)

	tasks := d.NextTasks()
	task := tasks[0]

	// Fail s1
	d.ReportUpdate(TaskUpdate{
		TaskID: task.ID,
		Status: "failed",
		Error:  "extract failed",
	})

	// Should now have the implicit input call (empty name)
	nextTasks := d.NextTasks()
	if len(nextTasks) != 1 {
		t.Fatalf("expected 1 task from handler, got %d", len(nextTasks))
	}

	emptyTask := nextTasks[0]
	if emptyTask.Step.DefinitionName != "" {
		t.Errorf("expected empty task, got %s", emptyTask.Step.DefinitionName)
	}

	// Complete empty task
	d.ReportUpdate(TaskUpdate{
		TaskID: emptyTask.ID,
		Status: "completed",
	})

	// Should now have r1
	handlerTasks := d.NextTasks()
	if len(handlerTasks) != 1 {
		t.Fatalf("expected 1 task after empty, got %d", len(handlerTasks))
	}

	handlerTask := handlerTasks[0]
	if handlerTask.Step.DefinitionName != "r1" {
		t.Errorf("expected handler task r1, got %s (ID: %s)", handlerTask.Step.DefinitionName, handlerTask.ID)
	}
}

func TestDispatcher_ComplexWorkflow(t *testing.T) {
	code := `
import "std/m" m

step s1 = m.extract
step s2 = m.filter
step s3 = m.output
step s4 = m.search
step r1 = m.retry

handler recover {
  *
    | r1
}

handler recover_local {
  *
    | r1
}

workflow main ? recover {
  s1 ? recover_local
    | s2
  > pipe_assignment

  pipe_assignment
    | s1
    | s2
  > pipe_assignment_2

  (from pipe_assignment join pipe_assignment_2 select o1)
    | s3

  s4
    | (from input join pipe_assignment_2 select o1)
    | r1
}
`
	c := compiler.New()
	program, err := c.Compile(code)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	d := NewDispatcher(program)

	// With DAG support, multiple statements can start in parallel if they don't have dependencies.
	tasks := d.NextTasks()
	if len(tasks) != 3 {
		t.Fatalf("expected 3 initial tasks (s1, s3's query, s4), got %d", len(tasks))
	}

	// We'll complete them one by one and check how it unfolds.

	// Helper to find task by step name
	findTask := func(tasks []Task, name string) *Task {
		for _, t := range tasks {
			if t.Step.DefinitionName == name {
				return &t
			}
		}
		return nil
	}

	// Helper to verify task properties
	checkTask := func(task *Task, name string, call []string) {
		if task == nil {
			t.Fatalf("task %s not found", name)
		}
		assert.Equal(t, name, task.Step.DefinitionName)
		assert.Equal(t, call, task.Step.Call)
	}

	// 1. Complete s1 (starts chain 1)
	s1Task := findTask(tasks, "s1")
	checkTask(s1Task, "s1", []string{"std/m", "extract"})
	d.ReportUpdate(TaskUpdate{TaskID: s1Task.ID, Status: "completed", OutputHandle: "h1"})

	// 2. Complete query (stmt 3 head)
	q1Task := findTask(tasks, "query")
	checkTask(q1Task, "query", []string{"std", "query"})
	d.ReportUpdate(TaskUpdate{TaskID: q1Task.ID, Status: "completed", OutputHandle: "hq1"})

	// 3. Complete s4 (stmt 4 head)
	s4Task := findTask(tasks, "s4")
	checkTask(s4Task, "s4", []string{"std/m", "search"})
	d.ReportUpdate(TaskUpdate{TaskID: s4Task.ID, Status: "completed", OutputHandle: "h4"})

	// Wave 2:
	tasks = d.NextTasks()
	// Should have:
	// - s2 (from chain 1, after s1)
	// - s3 (from chain 3, after query)
	// - query (from chain 4, after s4)
	assert.Len(t, tasks, 3)

	s2Task := findTask(tasks, "s2")
	checkTask(s2Task, "s2", []string{"std/m", "filter"})

	s3Task := findTask(tasks, "s3")
	checkTask(s3Task, "s3", []string{"std/m", "output"})

	q2Task := findTask(tasks, "query")
	checkTask(q2Task, "query", []string{"std", "query"})

	// Complete Wave 2
	d.ReportUpdate(TaskUpdate{TaskID: s2Task.ID, Status: "completed", OutputHandle: "h2"})
	d.ReportUpdate(TaskUpdate{TaskID: s3Task.ID, Status: "completed", OutputHandle: "h3"})
	d.ReportUpdate(TaskUpdate{TaskID: q2Task.ID, Status: "completed", OutputHandle: "hq2"})

	// Wave 3:
	tasks = d.NextTasks()
	// - s1 (from chain 2, after s2 completed and produced pipe_assignment)
	// - r1 (from chain 4, after query)
	assert.Len(t, tasks, 2)

	s1_2Task := findTask(tasks, "s1")
	checkTask(s1_2Task, "s1", []string{"std/m", "extract"})
	// Verify that the ticket from the previous assignment is present
	assert.Contains(t, s1_2Task.Tickets, "pipe_assignment")
	assert.Equal(t, "h2", s1_2Task.Tickets["pipe_assignment"].ResourceId)

	r1Task := findTask(tasks, "r1")
	checkTask(r1Task, "r1", []string{"std/m", "retry"})
	// r1 depends on the join query in stmt 4
	assert.Contains(t, r1Task.Tickets, "query_17") // query_17 is the auto-generated ID for the join
}

func TestDispatcher_Join(t *testing.T) {
	// Manually construct a DAG with a JOIN:
	// s1 \
	//      > s3
	// s2 /
	program := &ir.Program{
		Instructions: make(map[string]any),
		Workflows:    []string{"flow_0"},
	}

	s1 := &ir.StepInstruction{
		BaseInstruction: ir.BaseInstruction{ID: "s1", Type: ir.StepInst},
		DefinitionName:  "s1",
		Next:            []string{"s3"},
	}
	s2 := &ir.StepInstruction{
		BaseInstruction: ir.BaseInstruction{ID: "s2", Type: ir.StepInst},
		DefinitionName:  "s2",
		Next:            []string{"s3"},
	}
	s3 := &ir.StepInstruction{
		BaseInstruction: ir.BaseInstruction{ID: "s3", Type: ir.StepInst},
		DefinitionName:  "s3",
		Parents:         []string{"s1", "s2"},
	}
	flow := &ir.FlowInstruction{
		BaseInstruction: ir.BaseInstruction{ID: "flow_0", Type: ir.FlowInst},
		Heads:           []string{"s1", "s2"},
	}

	program.Instructions["s1"] = s1
	program.Instructions["s2"] = s2
	program.Instructions["s3"] = s3
	program.Instructions["flow_0"] = flow

	d := NewDispatcher(program)

	// Wave 1: s1 and s2 should be ready
	tasks := d.NextTasks()
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}

	// Complete s1
	d.ReportUpdate(TaskUpdate{TaskID: "s1", Status: "completed", OutputHandle: "h1"})

	// Only s2 should be in flight (s3 not ready yet)
	tasks = d.NextTasks()
	if len(tasks) != 0 {
		t.Fatalf("expected 0 new tasks, got %d", len(tasks))
	}

	// Complete s2
	d.ReportUpdate(TaskUpdate{TaskID: "s2", Status: "completed", OutputHandle: "h2"})

	// Now s3 should be ready
	tasks = d.NextTasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task (s3), got %d", len(tasks))
	}
	if tasks[0].ID != "s3" {
		t.Errorf("expected task s3, got %s", tasks[0].ID)
	}

	// Verify tickets
	if len(tasks[0].Tickets) != 2 {
		t.Errorf("expected 2 tickets for s3, got %d", len(tasks[0].Tickets))
	}
}
