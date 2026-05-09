package execution

import (
	"fmt"
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
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

	// In the current implementation, the lowerer linearizes all statements in a workflow.
	// This results in a single entry point (the first step of the first statement).
	tasks := d.NextTasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 initial task (due to linearization), got %d", len(tasks))
	}

	// Sequence of steps expected (matching TestLowerer_ComplexWorkflowAssignment):
	// 1. s1 (with local handler)
	// 2. s2 (assignment: pipe_assignment)
	// 3. s1
	// 4. s2 (assignment: pipe_assignment_2)
	// 5. query (Join)
	// 6. s3
	// 7. s4
	// 8. query (Join)
	// 9. r1

	expectedSteps := []struct {
		Name       string
		Assignment string
	}{
		{Name: "s1"},
		{Name: "s2", Assignment: "pipe_assignment"},
		{Name: "s1"},
		{Name: "s2", Assignment: "pipe_assignment_2"},
		{Name: "query"}, // Join
		{Name: "s3"},
		{Name: "s4"},
		{Name: "query"}, // Join
		{Name: "r1"},
	}

	for i, exp := range expectedSteps {
		if len(tasks) != 1 {
			t.Fatalf("Step %d: expected 1 task, got %d", i, len(tasks))
		}
		task := tasks[0]
		if task.Step.DefinitionName != exp.Name {
			t.Errorf("Step %d: expected %s, got %s", i, exp.Name, task.Step.DefinitionName)
		}
		
		// Report completion to move to the next task
		d.ReportUpdate(TaskUpdate{
			TaskID:       task.ID,
			Status:       "completed",
			OutputHandle: fmt.Sprintf("handle_%d", i),
		})

		// Check assignment recording if applicable
		if exp.Assignment != "" {
			if d.results[exp.Assignment] != fmt.Sprintf("handle_%d", i) {
				t.Errorf("Step %d: assignment %s not correctly recorded", i, exp.Assignment)
			}
		}

		tasks = d.NextTasks()
	}

	if len(tasks) != 0 {
		t.Errorf("expected no more tasks at the end, got %d", len(tasks))
	}
}
