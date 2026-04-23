package execution

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/compiler"
)

func TestDispatcher_BasicFlow(t *testing.T) {
	code := `
import "fhub/etl" etl
step s1: void -> void = etl.extract
step s2: void -> void = etl.transform

workflow main {
  s1 | s2
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
import "fhub/etl" etl
step s1: void -> void = etl.extract
step r1: void -> void = etl.retry

handler recover {
  r1
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

	// Should now have r1 (from handler)
	nextTasks := d.NextTasks()
	if len(nextTasks) != 1 {
		t.Fatalf("expected 1 task from handler, got %d", len(nextTasks))
	}

	handlerTask := nextTasks[0]
	if handlerTask.Step.DefinitionName != "r1" {
		t.Errorf("expected handler task r1, got %s", handlerTask.Step.DefinitionName)
	}
}
