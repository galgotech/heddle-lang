package recursive

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

type RecursiveOrchestrator struct {
	registry *registry.NodeRegistry
}

func (o *RecursiveOrchestrator) OrchestrateTask(ctx context.Context, task models.Task) {
	logger.L().Debug("workflow execution initiated: starting recursive task orchestration",
		logger.Component("control-plane"),
		logger.TraceID(task.ID),
		logger.ClientID(task.ClientID),
	)

	if task.ClientID == "" {
		logger.L().Error("task execution failed: task does not have a defined ClientID",
			logger.Component("control-plane"),
			logger.TraceID(task.ID),
		)
		return
	}

	o.registry.RegisterWorkflowClient(task.ID, task.ClientID)
	defer o.registry.DeregisterWorkflowClient(task.ID)

	program := task.Program
	clientStream, ok := o.registry.GetNode(task.ClientID)
	if !ok {
		logger.L().Warn("client stream warning: stream will not be sent to client",
			logger.Component("control-plane"),
			logger.TraceID(task.ID),
			logger.ClientID(task.ClientID),
		)
	}

	var stream transport.ExchangeStream
	if clientStream != nil {
		stream = clientStream.GetStream()
	}

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

		logger.L().Info("workflow execution started: executing flow",
			logger.Component("control-plane"),
			logger.TraceID(task.ID),
			logger.String("flow_name", flow.Name),
		)

		if clientStream != nil {
			clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Starting execution of workflow %s...", flow.Name)})
		}

		var runErr error
		for _, headID := range flow.Heads {
			if err := o.executeStepRecursive(ctx, task.ID, program, headID, nil, task.Schemas, stream); err != nil {
				runErr = err
				break
			}
		}

		if runErr != nil {
			logger.L().Error("workflow execution failed: task encountered execution errors",
				logger.Component("control-plane"),
				logger.TraceID(task.ID),
				logger.Error(runErr),
			)
			if clientStream != nil {
				clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Workflow failed: %v", runErr)})
			}
			return
		}
	}

	logger.L().Info("workflow execution completed: task finished successfully",
		logger.Component("control-plane"),
		logger.TraceID(task.ID),
	)
	if clientStream != nil {
		clientStream.Send(&transport.FlightData{DataBody: []byte("LOG:Workflow completed successfully.")})
	}
}

func (o *RecursiveOrchestrator) executeStepRecursive(ctx context.Context, workflowID string, prog ir.Program, stepID string, prevTaskIDs []string, schemas map[string]schema.StepSchemas, clientStream transport.ExchangeStream) error {
	logger.L().Debug("step execution initiated: preparing to execute step",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(stepID),
		logger.Any("prev_task_ids", prevTaskIDs),
	)

	// 0. Validate Schema Compatibility for all edges
	for _, parentID := range prevTaskIDs {
		if err := orchestrator.ValidateEdge(prog, parentID, stepID, schemas); err != nil {
			logger.L().Error("step validation failed: edge schema validation error",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
				logger.Error(err),
			)
			if clientStream != nil {
				clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Validation failed for step %s: %v", stepID, err)})
			}
			return err
		}
	}

	step, ok := prog.Instructions[stepID].(ir.StepInstruction)
	if !ok {
		err := fmt.Errorf("step %s is not a valid instruction", stepID)
		logger.L().Error("step validation failed: step is not a valid instruction type",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
			logger.Error(err),
		)
		return err
	}
	capability := fmt.Sprintf("%s.%s", step.Call[0], step.Call[1])

	if clientStream != nil {
		clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Executing step %s (%s)...", stepID, capability)})
	}

	// 1. Find worker
	workerStream := o.registry.FindWorkerByCapability(capability)
	if workerStream == nil {
		err := fmt.Errorf("no worker found for capability: %s", capability)
		logger.L().Error("step dispatch failed: worker capability not registered",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
			logger.Capability(capability),
			logger.Error(err),
		)
		return err
	}

	logger.L().Debug("step dispatching: worker node selected for capability execution",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(stepID),
		logger.WorkerID(workerStream.GetID()),
		logger.Capability(capability),
	)

	// 2. Create result future and register it
	future := models.NewTaskFuture()
	workerStream.RegisterResultFuture(stepID, future)
	defer workerStream.DeregisterResultFuture(stepID)

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
		TaskID:            stepID,
		PreviousTaskID:    prevTaskID,
		PreviousTaskIDs:   prevTaskIDs,
		ParentAssignments: parentAssignments,
		Step:              step,
		Resources:         orchestrator.ResolveResources(prog, step),
	}
	body, err := json.Marshal(execTask)
	if err != nil {
		err = fmt.Errorf("failed to marshal step: %w", err)
		logger.L().Error("step dispatch failed: failed to marshal step execution task",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
			logger.Error(err),
		)
		return err
	}
	if err := workerStream.Send(&transport.FlightData{DataBody: body}); err != nil {
		err = fmt.Errorf("failed to send step to worker %s: %w", workerStream.GetID(), err)
		logger.L().Error("step dispatch failed: failed to send step execution to worker",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
			logger.WorkerID(workerStream.GetID()),
			logger.Error(err),
		)
		return err
	}

	logger.L().Debug("step dispatched: execution task sent successfully",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(stepID),
		logger.WorkerID(workerStream.GetID()),
	)

	// 5. Wait for result
	logger.L().Debug("step awaiting: waiting for worker execution result",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(stepID),
	)

	var stepErr error
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res, err := future.Await(timeoutCtx)
	if err != nil {
		if err == context.DeadlineExceeded {
			stepErr = fmt.Errorf("step %s timed out", stepID)
			logger.L().Error("step execution failed: task execution timed out",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
				logger.Error(stepErr),
			)
		} else {
			stepErr = err
			logger.L().Error("step execution failed: error while awaiting worker task completion",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
				logger.Error(stepErr),
			)
		}
	} else if res.Status != models.TaskStatusSuccess {
		stepErr = fmt.Errorf("step %s failed: %s", stepID, res.ErrorMessage)
		logger.L().Error("step execution failed: worker reported task execution failure",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
			logger.String("error_message", res.ErrorMessage),
		)
	}

	if stepErr != nil {
		if clientStream != nil {
			clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Step %s failed: %v", stepID, stepErr)})
		}
		return stepErr
	}

	logger.L().Info("step execution completed: step completed successfully",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(stepID),
	)

	if clientStream != nil {
		clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Step %s completed successfully.", stepID)})
	}

	// 6. Continue to next steps
	for _, nextID := range step.Next {
		logger.L().Debug("step transitions: continuing execution to downstream step",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
			logger.String("next_step_id", nextID),
		)
		var childPrevIDs []string
		if childInst, ok := prog.Instructions[nextID]; ok {
			if childStep, ok := childInst.(ir.StepInstruction); ok {
				childPrevIDs = childStep.Parents
			}
		}
		if len(childPrevIDs) == 0 {
			childPrevIDs = []string{stepID}
		}

		if err := o.executeStepRecursive(ctx, workflowID, prog, nextID, childPrevIDs, schemas, clientStream); err != nil {
			return err
		}
	}

	return nil
}

func NewRecursiveOrchestrator(registry *registry.NodeRegistry) *RecursiveOrchestrator {
	return &RecursiveOrchestrator{registry: registry}
}
