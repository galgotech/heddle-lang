package debug

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/galgotech/heddle-lang/internal/controlplane/orchestrator"
	"github.com/galgotech/heddle-lang/internal/controlplane/registry"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/pkg/schema"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

type DebugOrchestrator struct {
	registry *registry.NodeRegistry
}

func NewDebugOrchestrator(registry *registry.NodeRegistry) *DebugOrchestrator {
	return &DebugOrchestrator{registry: registry}
}

func (o *DebugOrchestrator) OrchestrateTask(ctx context.Context, task models.Task) {
	logger.L().Debug("workflow execution initiated: starting debug task orchestration",
		logger.Component("control-plane"),
		logger.TraceID(task.ID),
		logger.ClientID(task.ClientID),
	)

	if task.ClientID != "" {
		o.registry.RegisterWorkflowClient(task.ID, task.ClientID)
		defer o.registry.DeregisterWorkflowClient(task.ID)
	}

	program := task.Program
	clientStream, ok := o.registry.GetNode(task.ClientID)
	if !ok {
		logger.L().Error("client stream lookup failed: client stream not registered in node registry",
			logger.Component("control-plane"),
			logger.TraceID(task.ID),
			logger.ClientID(task.ClientID),
		)
		return
	}

	var stream transport.ExchangeStream
	if clientStream != nil {
		stream = clientStream.GetStream()
	}

	// Thread-safe map to collect output handles of executed steps
	var mu sync.RWMutex
	allOutputs := make(map[string]map[string]string) // stepID -> fieldName -> SHMPath

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

		if task.TargetWorkflow != "" && flow.Name != task.TargetWorkflow {
			continue
		}

		logger.L().Info("workflow execution started: executing flow in debug mode",
			logger.Component("control-plane"),
			logger.TraceID(task.ID),
			logger.String("flow_name", flow.Name),
		)
		if clientStream != nil {
			err := clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Starting debug execution of workflow %s...", flow.Name)})
			if err != nil {
				logger.L().Error("client communication failed: failed to send debug start confirmation to client",
					logger.Component("control-plane"),
					logger.TraceID(task.ID),
					logger.Error(err),
				)
				break
			}
		}

		var runErr error
		for _, headID := range flow.Heads {
			if err := o.executeStepDebug(ctx, task.ID, program, headID, nil, task.Schemas, stream, allOutputs, &mu); err != nil {
				runErr = err
				break
			}
		}

		if runErr != nil {
			logger.L().Error("workflow execution failed: task encountered execution errors in debug mode",
				logger.Component("control-plane"),
				logger.TraceID(task.ID),
				logger.Error(runErr),
			)
			if clientStream != nil {
				err := clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Workflow failed: %v", runErr)})
				if err != nil {
					logger.L().Error("client communication failed: failed to send debug failure report to client",
						logger.Component("control-plane"),
						logger.TraceID(task.ID),
						logger.Error(err),
					)
					break
				}
			}
			return
		}
	}

	logger.L().Info("workflow execution completed: task finished successfully in debug mode",
		logger.Component("control-plane"),
		logger.TraceID(task.ID),
	)
	if clientStream != nil {
		err := clientStream.Send(&transport.FlightData{DataBody: []byte("LOG:Workflow completed successfully.")})
		if err != nil {
			logger.L().Error("client communication failed: failed to send debug completion notification to client",
				logger.Component("control-plane"),
				logger.TraceID(task.ID),
				logger.Error(err),
			)
		}
	}
}

func (o *DebugOrchestrator) executeStepDebug(
	ctx context.Context,
	workflowID string,
	prog ir.Program,
	stepID string,
	prevTaskIDs []string,
	schemas map[string]schema.StepSchemas,
	clientStream transport.ExchangeStream,
	allOutputs map[string]map[string]string,
	mu *sync.RWMutex,
) error {
	logger.L().Debug("step execution initiated: preparing to execute step in debug mode",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(stepID),
		logger.Any("prev_task_ids", prevTaskIDs),
	)

	// 0. Validate Schema Compatibility for all edges
	for _, parentID := range prevTaskIDs {
		if err := orchestrator.ValidateEdge(prog, parentID, stepID, schemas); err != nil {
			logger.L().Error("step validation failed: edge schema validation error in debug mode",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
				logger.Error(err),
			)
			if clientStream != nil {
				_ = clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Validation failed for step %s: %v", stepID, err)})
			}
			return err
		}
	}

	step, ok := prog.Instructions[stepID].(ir.StepInstruction)
	if !ok {
		err := fmt.Errorf("step %s is not a valid instruction", stepID)
		logger.L().Error("step validation failed: step is not a valid instruction type in debug mode",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
			logger.Error(err),
		)
		return err
	}
	capability := fmt.Sprintf("%s.%s", step.Call[0], step.Call[1])

	// 1. Generate Input Previews by collecting outputs from all parent task IDs
	parentOutputs := make(map[string]string)
	for _, pID := range prevTaskIDs {
		mu.RLock()
		outs := allOutputs[pID]
		mu.RUnlock()

		// If there is an assignment name for this parent, we prefix the keys
		var assignmentName string
		if parentInst, ok := prog.Instructions[pID]; ok {
			if parentStep, ok := parentInst.(ir.StepInstruction); ok {
				assignmentName = parentStep.Assignment
			}
		}

		for k, v := range outs {
			key := k
			if assignmentName != "" {
				key = fmt.Sprintf("%s_%s", assignmentName, k)
			}
			parentOutputs[key] = v
		}
	}

	inputPreviews := make(map[string]string)
	for fieldName, shmPath := range parentOutputs {
		if shmPath != "" {
			if preview, err := locality.FormatArrowPreview(shmPath); err == nil {
				inputPreviews[fieldName] = preview
			} else {
				// Fallback to schema if SHM file is inaccessible or errors
				logger.L().Warn("preview formatting warning: failed to format input preview",
					logger.Component("control-plane"),
					logger.TraceID(workflowID),
					logger.TaskID(stepID),
					logger.String("field_name", fieldName),
					logger.String("shm_path", shmPath),
					logger.Error(err),
				)
				inputPreviews[fieldName] = fmt.Sprintf("<error formatting preview: %v>", err)
			}
		}
	}
	inputsJSON, _ := json.Marshal(inputPreviews)

	// 2. Pause and Prompt Client (DAP Server)
	line := 0
	col := 0
	if step.SourceLocation != nil {
		line = step.SourceLocation.Line
		col = step.SourceLocation.Column
	}

	logger.L().Info("debug breakpoint reached: paused execution at step",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(stepID),
		logger.Int("line", line),
		logger.Int("col", col),
	)

	if clientStream != nil {
		// Send DEBUG_PAUSED message
		pausedHeader := fmt.Sprintf("DEBUG_PAUSED:%s:%d:%d:%s", stepID, line, col, string(inputsJSON))
		if err := clientStream.Send(&transport.FlightData{DataBody: []byte(pausedHeader)}); err != nil {
			err = fmt.Errorf("failed to send debug pause to client: %w", err)
			logger.L().Error("breakpoint pause failed: failed to send debug pause to client stream",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
				logger.Error(err),
			)
			return err
		}

		logger.L().Debug("breakpoint pause: waiting for debug command from client",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
		)

		// Block and wait for client command (STEP or STOP)
		msg, err := clientStream.Recv()
		if err != nil {
			err = fmt.Errorf("failed to receive debug command from client: %w", err)
			logger.L().Error("breakpoint command failed: error receiving debug command from client stream",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
				logger.Error(err),
			)
			return err
		}

		cmd := string(msg.DataBody)
		if cmd == "STOP" {
			err = fmt.Errorf("debug session stopped by user")
			logger.L().Warn("breakpoint resume: debug session stopped by user command",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
			)
			return err
		}
		logger.L().Info("breakpoint resume: step execution resumed by client",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
		)
	} else {
		// Headless/test mode simulator
		logger.L().Debug("breakpoint simulation: awaiting simulated debug step resume",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
		)
		select {
		case <-ctx.Done():
			err := ctx.Err()
			logger.L().Error("breakpoint simulation failed: context cancelled during headless debug resume simulation",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
				logger.Error(err),
			)
			return err
		case <-time.After(10 * time.Millisecond):
			logger.L().Info("breakpoint simulation: step auto-resumed in headless mode",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
			)
		}
	}

	// 3. Find worker and dispatch
	workerStream := o.registry.FindWorkerByCapability(capability)
	if workerStream == nil {
		err := fmt.Errorf("no worker found for capability: %s", capability)
		logger.L().Error("step dispatch failed: worker capability not registered in debug mode",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
			logger.Capability(capability),
			logger.Error(err),
		)
		return err
	}

	logger.L().Debug("step dispatching: worker node selected for capability execution in debug mode",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(stepID),
		logger.WorkerID(workerStream.GetID()),
		logger.Capability(capability),
	)

	future := models.NewTaskFuture()
	workerStream.RegisterResultFuture(stepID, future)
	defer workerStream.DeregisterResultFuture(stepID)

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
		logger.L().Error("step dispatch failed: failed to marshal step execution task in debug mode",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
			logger.Error(err),
		)
		return err
	}
	if err := workerStream.Send(&transport.FlightData{DataBody: body}); err != nil {
		err = fmt.Errorf("failed to send step to worker %s: %w", workerStream.GetID(), err)
		logger.L().Error("step dispatch failed: failed to send step execution to worker in debug mode",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
			logger.WorkerID(workerStream.GetID()),
			logger.Error(err),
		)
		return err
	}

	logger.L().Debug("step dispatched: execution task sent successfully in debug mode",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(stepID),
		logger.WorkerID(workerStream.GetID()),
	)

	// 4. Wait for result
	logger.L().Debug("step awaiting: waiting for worker execution result in debug mode",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(stepID),
	)

	var stepErr error
	var taskRes models.TaskResult
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	taskRes, err = future.Await(timeoutCtx)
	if err != nil {
		if err == context.DeadlineExceeded {
			stepErr = fmt.Errorf("step %s timed out", stepID)
			logger.L().Error("step execution failed: task execution timed out in debug mode",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
				logger.Error(stepErr),
			)
		} else {
			stepErr = err
			logger.L().Error("step execution failed: error while awaiting worker task completion in debug mode",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
				logger.Error(stepErr),
			)
		}
	} else if taskRes.Status != models.TaskStatusSuccess {
		stepErr = fmt.Errorf("step %s failed: %s", stepID, taskRes.ErrorMessage)
		logger.L().Error("step execution failed: worker reported task execution failure in debug mode",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
			logger.TaskID(stepID),
			logger.String("error_message", taskRes.ErrorMessage),
		)
	}

	if stepErr != nil {
		if clientStream != nil {
			_ = clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Step %s failed: %v", stepID, stepErr)})
		}
		return stepErr
	}

	logger.L().Info("step execution completed: step completed successfully in debug mode",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.TaskID(stepID),
	)

	// 5. Store physical output paths in our map
	mu.Lock()
	if taskRes.OutputHandles != nil {
		allOutputs[stepID] = taskRes.OutputHandles
	} else {
		allOutputs[stepID] = make(map[string]string)
	}
	stepOutputs := allOutputs[stepID]
	mu.Unlock()

	// 6. Generate Output Previews
	outputPreviews := make(map[string]string)
	for fieldName, shmPath := range stepOutputs {
		if shmPath != "" {
			if preview, err := locality.FormatArrowPreview(shmPath); err == nil {
				outputPreviews[fieldName] = preview
			} else {
				logger.L().Warn("preview formatting warning: failed to format output preview",
					logger.Component("control-plane"),
					logger.TraceID(workflowID),
					logger.TaskID(stepID),
					logger.String("field_name", fieldName),
					logger.String("shm_path", shmPath),
					logger.Error(err),
				)
				outputPreviews[fieldName] = fmt.Sprintf("<error formatting preview: %v>", err)
			}
		}
	}
	outputsJSON, _ := json.Marshal(outputPreviews)

	// 7. Report step completion with outputs
	if clientStream != nil {
		completeHeader := fmt.Sprintf("DEBUG_STEP_COMPLETE:%s:%s:%s", stepID, taskRes.Status, string(outputsJSON))
		if err := clientStream.Send(&transport.FlightData{DataBody: []byte(completeHeader)}); err != nil {
			err = fmt.Errorf("failed to send debug completion to client: %w", err)
			logger.L().Error("client communication failed: failed to send debug step completion report to client",
				logger.Component("control-plane"),
				logger.TraceID(workflowID),
				logger.TaskID(stepID),
				logger.Error(err),
			)
			return err
		}
	}

	// 8. Recurse to next steps
	for _, nextID := range step.Next {
		logger.L().Debug("step transitions: continuing execution to downstream step in debug mode",
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

		if err := o.executeStepDebug(ctx, workflowID, prog, nextID, childPrevIDs, schemas, clientStream, allOutputs, mu); err != nil {
			return err
		}
	}

	return nil
}
