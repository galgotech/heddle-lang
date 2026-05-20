package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator"
	"github.com/galgotech/heddle-lang/internal/control-plane/registry"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

type GraphOrchestrator struct {
	registry *registry.WorkerRegistry
}

func (o *GraphOrchestrator) OrchestrateTask(ctx context.Context, task models.Task) {
	if task.ClientID != "" {
		o.registry.RegisterWorkflowClient(task.ID, task.ClientID)
		defer o.registry.DeregisterWorkflowClient(task.ID)
	}

	program := task.Program
	for _, flowID := range program.Workflows {
		flow := program.Instructions[flowID].(*ir.FlowInstruction)

		// Filter by workflow name if specified
		if task.TargetWorkflow != "" && flow.Name != task.TargetWorkflow {
			continue
		}

		if err := o.executeGraph(ctx, task.ID, program, flow, task.Schemas); err != nil {
			logger.L().Error("Task failed in graph mode", zap.Error(err))
			return
		}
	}
	logger.L().Info("Task completed successfully in graph mode", zap.String("id", task.ID))
}

func (o *GraphOrchestrator) executeGraph(ctx context.Context, workflowID string, prog *ir.Program, flow *ir.FlowInstruction, schemas map[string]schema.StepSchemas) error {
	// Build map of all steps in the flow
	steps := make(map[string]ir.StepInstruction)
	inDegree := make(map[string]int)

	// Helper to find all steps reachable from heads
	var collectSteps func(id string)
	collectSteps = func(id string) {
		if _, ok := steps[id]; ok {
			return
		}
		inst, ok := prog.Instructions[id]
		if !ok {
			return
		}
		step, ok := inst.(ir.StepInstruction)
		if !ok {
			return
		}
		steps[id] = step
		for _, nextID := range step.Next {
			inDegree[nextID]++
			collectSteps(nextID)
		}
	}

	for _, headID := range flow.Heads {
		collectSteps(headID)
	}

	// Ready queue of step IDs (in-degree == 0)
	var readyQueue []string
	for _, headID := range flow.Heads {
		if inDegree[headID] == 0 {
			readyQueue = append(readyQueue, headID)
		}
	}

	// Keep track of parent relationship for edge validation
	parents := make(map[string]string)
	for _, headID := range flow.Heads {
		parents[headID] = ""
	}
	for _, step := range steps {
		for _, nextID := range step.Next {
			parents[nextID] = step.ID
		}
	}

	completedCount := 0
	totalSteps := len(steps)

	for len(readyQueue) > 0 {
		// Pop step
		currentID := readyQueue[0]
		readyQueue = readyQueue[1:]

		step := steps[currentID]
		prevID := parents[currentID]

		// Execute step
		if err := o.executeStep(ctx, workflowID, prog, step, prevID, schemas); err != nil {
			return err
		}
		completedCount++

		// Decrement in-degree for children
		for _, nextID := range step.Next {
			inDegree[nextID]--
			if inDegree[nextID] == 0 {
				readyQueue = append(readyQueue, nextID)
			}
		}
	}

	if completedCount < totalSteps {
		return fmt.Errorf("cycle detected in execution graph: executed %d of %d steps", completedCount, totalSteps)
	}

	return nil
}

func (o *GraphOrchestrator) executeStep(ctx context.Context, workflowID string, prog *ir.Program, step ir.StepInstruction, prevTaskID string, schemas map[string]schema.StepSchemas) error {
	// 0. Validate Schema Compatibility
	if err := orchestrator.ValidateEdge(prog, prevTaskID, step.ID, schemas); err != nil {
		return err
	}

	capability := fmt.Sprintf("%s.%s", step.Call[0], step.Call[1])

	// 1. Find worker
	worker := o.registry.FindWorkerStreamForStep(capability)
	if worker == nil {
		return fmt.Errorf("no worker found for capability: %s", capability)
	}

	workerStream, ok := o.registry.GetActiveWorkerStream(worker.GetID())
	if !ok {
		return fmt.Errorf("worker %s stream not found", worker.GetID())
	}

	// 3. Create result channel and register it
	resultCh := make(chan models.TaskResult, 1)
	o.registry.RegisterResultChan(step.ID, resultCh)
	defer o.registry.DeregisterResultChan(step.ID)

	// 4. Dispatch step
	execTask := models.StepExecutionTask{
		WorkflowID:     workflowID,
		TaskID:         step.ID,
		PreviousTaskID: prevTaskID,
		Step:           step,
	}
	body, err := json.Marshal(execTask)
	if err != nil {
		return fmt.Errorf("failed to marshal step: %w", err)
	}
	if err := workerStream.Send(&flight.FlightData{DataBody: body}); err != nil {
		return fmt.Errorf("failed to send step to worker %s: %w", worker.GetID(), err)
	}

	// 5. Wait for result
	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-resultCh:
		if res.Status != models.TaskStatusSuccess {
			return fmt.Errorf("step %s failed: %s", step.ID, res.ErrorMessage)
		}
	case <-time.After(30 * time.Second):
		return fmt.Errorf("step %s timed out", step.ID)
	}

	return nil
}

func NewGraphOrchestrator(registry *registry.WorkerRegistry) *GraphOrchestrator {
	return &GraphOrchestrator{registry: registry}
}
