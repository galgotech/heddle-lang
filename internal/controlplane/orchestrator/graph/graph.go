package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/galgotech/heddle-lang/internal/controlplane/orchestrator"
	"github.com/galgotech/heddle-lang/internal/controlplane/registry"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/schema"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

type GraphOrchestrator struct {
	registry *registry.NodeRegistry
}

func (o *GraphOrchestrator) OrchestrateTask(ctx context.Context, task models.Task) {
	logger.L().Debug("workflow execution initiated: starting graph task orchestration",
		logger.Component("control-plane"),
		logger.TraceID(task.ID),
		logger.ClientID(task.ClientID),
	)

	if task.ClientID != "" {
		o.registry.RegisterWorkflowClient(task.ID, task.ClientID)
		defer o.registry.DeregisterWorkflowClient(task.ID)
	}

	program := task.Program
	for _, flowID := range program.Workflows {
		inst := program.Instructions[flowID]
		var flow ir.FlowInstruction
		switch f := inst.(type) {
		case ir.FlowInstruction:
			flow = f
		case *ir.FlowInstruction:
			flow = *f
		default:
			logger.L().Error("instruction parsing failed: flow is not a valid FlowInstruction",
				logger.Component("control-plane"),
				logger.TraceID(task.ID),
				logger.String("flow_id", flowID),
			)
			continue
		}

		// Filter by workflow name if specified
		if task.TargetWorkflow != "" && flow.Name != task.TargetWorkflow {
			continue
		}

		logger.L().Info("workflow execution started: executing flow in graph mode",
			logger.Component("control-plane"),
			logger.TraceID(task.ID),
			logger.String("flow_name", flow.Name),
		)

		if err := o.executeGraph(ctx, task.ID, program, flow, task.Schemas); err != nil {
			logger.L().Error("workflow execution failed: task encountered execution errors in graph mode",
				logger.Component("control-plane"),
				logger.TraceID(task.ID),
				logger.Error(err),
			)
			return
		}
	}
	logger.L().Info("workflow execution completed: task finished successfully in graph mode",
		logger.Component("control-plane"),
		logger.TraceID(task.ID),
	)
}

func (o *GraphOrchestrator) executeGraph(ctx context.Context, workflowID string, prog ir.Program, flow ir.FlowInstruction, schemas map[string]schema.StepSchemas) error {
	logger.L().Debug("graph execution initiated: building step dependency map",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.String("flow_name", flow.Name),
	)

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
	parents := make(map[string][]string)
	for _, headID := range flow.Heads {
		parents[headID] = []string{}
	}
	for _, step := range steps {
		for _, nextID := range step.Next {
			parents[nextID] = append(parents[nextID], step.ID)
		}
	}

	completedCount := 0
	totalSteps := len(steps)

	for len(readyQueue) > 0 {
		// Pop step
		currentID := readyQueue[0]
		readyQueue = readyQueue[1:]

		step := steps[currentID]
		prevIDs := parents[currentID]

		logger.L().Debug("graph execution processing: executing step from ready queue",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(currentID),
		)

		// Execute step
		if err := o.executeStep(ctx, workflowID, prog, step, prevIDs, schemas); err != nil {
			return err
		}
		completedCount++

		// Decrement in-degree for children
		for _, nextID := range step.Next {
			inDegree[nextID]--
			if inDegree[nextID] == 0 {
				logger.L().Debug("graph execution processing: step unlocked and added to ready queue",
					logger.Component("control-plane"),
					logger.TraceID(workflowID),
					logger.TaskID(nextID),
				)
				readyQueue = append(readyQueue, nextID)
			}
		}
	}

	if completedCount < totalSteps {
		err := fmt.Errorf("cycle detected in execution graph: executed %d of %d steps", completedCount, totalSteps)
		logger.L().Error("graph execution failed: cycle detected in step dependencies",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.Int("completed_count", completedCount),
			logger.Int("total_steps", totalSteps),
			logger.Error(err),
		)
		return err
	}

	return nil
}

func (o *GraphOrchestrator) executeStep(ctx context.Context, workflowID string, prog ir.Program, step ir.StepInstruction, prevTaskIDs []string, schemas map[string]schema.StepSchemas) error {
	logger.L().Debug("step execution initiated: preparing to execute step in graph mode",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(step.ID),
		logger.Any("prev_task_ids", prevTaskIDs),
	)

	// 0. Validate Schema Compatibility for all edges
	for _, parentID := range prevTaskIDs {
		if err := orchestrator.ValidateEdge(prog, parentID, step.ID, schemas); err != nil {
			logger.L().Error("step validation failed: edge schema validation error in graph mode",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(step.ID),
				logger.Error(err),
			)
			return err
		}
	}

	capability := fmt.Sprintf("%s.%s", step.Call[0], step.Call[1])

	// 1. Find worker
	workerStream := o.registry.FindWorkerByCapability(capability)
	if workerStream == nil {
		err := fmt.Errorf("no worker found for capability: %s", capability)
		logger.L().Error("step dispatch failed: worker capability not registered in graph mode",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(step.ID),
			logger.Capability(capability),
			logger.Error(err),
		)
		return err
	}

	logger.L().Debug("step dispatching: worker node selected for capability execution in graph mode",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(step.ID),
		logger.WorkerID(workerStream.GetID()),
		logger.Capability(capability),
	)

	// 3. Create result future and register it
	future := models.NewTaskFuture()
	workerStream.RegisterResultFuture(step.ID, future)
	defer workerStream.DeregisterResultFuture(step.ID)

	// 4. Dispatch step
	var prevTaskID string
	if len(prevTaskIDs) > 0 {
		prevTaskID = prevTaskIDs[0]
	}

	parentAssignments := make(map[string]string)
	for _, pID := range prevTaskIDs {
		if pID != "" {
			if parentInst, ok := prog.Instructions[pID]; ok {
				if parentStep, ok := parentInst.(ir.StepInstruction); ok {
					if parentStep.Assignment != "" {
						parentAssignments[pID] = parentStep.Assignment
					}
				}
			}
		}
	}

	execTask := models.StepExecutionTask{
		WorkflowID:        workflowID,
		TaskID:            step.ID,
		PreviousTaskID:    prevTaskID,
		PreviousTaskIDs:   prevTaskIDs,
		ParentAssignments: parentAssignments,
		Step:              step,
		Resources:         orchestrator.ResolveResources(prog, step),
	}
	body, err := json.Marshal(execTask)
	if err != nil {
		err = fmt.Errorf("failed to marshal step: %w", err)
		logger.L().Error("step dispatch failed: failed to marshal step execution task in graph mode",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(step.ID),
			logger.Error(err),
		)
		return err
	}
	if err := workerStream.Send(&transport.FlightData{DataBody: body}); err != nil {
		err = fmt.Errorf("failed to send step to worker %s: %w", workerStream.GetID(), err)
		logger.L().Error("step dispatch failed: failed to send step execution to worker in graph mode",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(step.ID),
			logger.WorkerID(workerStream.GetID()),
			logger.Error(err),
		)
		return err
	}

	logger.L().Debug("step dispatched: execution task sent successfully in graph mode",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(step.ID),
		logger.WorkerID(workerStream.GetID()),
	)

	// 5. Wait for result
	logger.L().Debug("step awaiting: waiting for worker execution result in graph mode",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(step.ID),
	)

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res, err := future.Await(timeoutCtx)
	if err != nil {
		var stepErr error
		if err == context.DeadlineExceeded {
			stepErr = fmt.Errorf("step %s timed out", step.ID)
			logger.L().Error("step execution failed: task execution timed out in graph mode",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(step.ID),
				logger.Error(stepErr),
			)
		} else {
			stepErr = err
			logger.L().Error("step execution failed: error while awaiting worker task completion in graph mode",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(step.ID),
				logger.Error(stepErr),
			)
		}
		return stepErr
	}
	if res.Status != models.TaskStatusSuccess {
		err = fmt.Errorf("step %s failed: %s", step.ID, res.ErrorMessage)
		logger.L().Error("step execution failed: worker reported task execution failure in graph mode",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(step.ID),
			logger.String("error_message", res.ErrorMessage),
			logger.Error(err),
		)
		return err
	}

	logger.L().Info("step execution completed: step completed successfully in graph mode",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(step.ID),
	)

	return nil
}

func NewGraphOrchestrator(registry *registry.NodeRegistry) *GraphOrchestrator {
	return &GraphOrchestrator{registry: registry}
}
