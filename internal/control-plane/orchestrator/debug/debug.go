package debug

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator"
	"github.com/galgotech/heddle-lang/internal/control-plane/registry"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/pkg/schema"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

type DebugOrchestrator struct {
	registry *registry.WorkerRegistry
}

func NewDebugOrchestrator(registry *registry.WorkerRegistry) *DebugOrchestrator {
	return &DebugOrchestrator{registry: registry}
}

func (o *DebugOrchestrator) OrchestrateTask(ctx context.Context, task models.Task) {
	if task.ClientID != "" {
		o.registry.RegisterWorkflowClient(task.ID, task.ClientID)
		defer o.registry.DeregisterWorkflowClient(task.ID)
	}

	program := task.Program
	clientStream, ok := o.registry.GetActiveClientStream(task.ClientID)
	if !ok {
		logger.L().Error("Client not found", logger.String("id", task.ClientID))
		return
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
			logger.L().Error("flow is not a valid FlowInstruction", logger.String("id", flowID))
			continue
		}

		if task.TargetWorkflow != "" && flow.Name != task.TargetWorkflow {
			continue
		}

		logger.L().Info("[DEBUG] Starting debug workflow execution", logger.String("workflow", flow.Name))
		if clientStream != nil {
			err := clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Starting debug execution of workflow %s...", flow.Name)})
			if err != nil {
				logger.L().Error("[DEBUG] Failed to send debug start to client", logger.Error(err))
				break
			}
		}

		var runErr error
		for _, headID := range flow.Heads {
			if err := o.executeStepDebug(ctx, task.ID, program, headID, "", task.Schemas, clientStream, allOutputs, &mu); err != nil {
				runErr = err
				break
			}
		}

		if runErr != nil {
			logger.L().Error("[DEBUG] Workflow execution failed", logger.Error(runErr))
			if clientStream != nil {
				err := clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Workflow failed: %v", runErr)})
				if err != nil {
					logger.L().Error("[DEBUG] Failed to send debug pause to client", logger.Error(err))
					break
				}
			}
			return
		}
	}

	logger.L().Info("[DEBUG] Workflow execution completed successfully", logger.String("id", task.ID))
	if clientStream != nil {
		err := clientStream.Send(&transport.FlightData{DataBody: []byte("LOG:Workflow completed successfully.")})
		if err != nil {
			logger.L().Error("[DEBUG] Failed to send debug pause to client", logger.Error(err))
		}
	}
}

func (o *DebugOrchestrator) executeStepDebug(
	ctx context.Context,
	workflowID string,
	prog ir.Program,
	stepID string,
	prevTaskID string,
	schemas map[string]schema.StepSchemas,
	clientStream transport.ExchangeStream,
	allOutputs map[string]map[string]string,
	mu *sync.RWMutex,
) error {
	if err := orchestrator.ValidateEdge(prog, prevTaskID, stepID, schemas); err != nil {
		if clientStream != nil {
			_ = clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Validation failed for step %s: %v", stepID, err)})
		}
		return err
	}

	step, ok := prog.Instructions[stepID].(ir.StepInstruction)
	if !ok {
		return fmt.Errorf("step %s is not a valid instruction", stepID)
	}
	capability := fmt.Sprintf("%s.%s", step.Call[0], step.Call[1])

	// 1. Generate Input Previews
	mu.RLock()
	parentOutputs := allOutputs[prevTaskID]
	mu.RUnlock()

	inputPreviews := make(map[string]string)
	for fieldName, shmPath := range parentOutputs {
		if shmPath != "" {
			if preview, err := locality.FormatArrowPreview(shmPath); err == nil {
				inputPreviews[fieldName] = preview
			} else {
				// Fallback to schema if SHM file is inaccessible or errors
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

	logger.L().Info("[DEBUG] Paused at step", logger.String("step_id", stepID), logger.Int("line", line), logger.Int("col", col))

	if clientStream != nil {
		// Send DEBUG_PAUSED message
		pausedHeader := fmt.Sprintf("DEBUG_PAUSED:%s:%d:%d:%s", stepID, line, col, string(inputsJSON))
		if err := clientStream.Send(&transport.FlightData{DataBody: []byte(pausedHeader)}); err != nil {
			return fmt.Errorf("failed to send debug pause to client: %w", err)
		}

		// Block and wait for client command (STEP or STOP)
		msg, err := clientStream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive debug command from client: %w", err)
		}

		cmd := string(msg.DataBody)
		if cmd == "STOP" {
			return fmt.Errorf("debug session stopped by user")
		}
		logger.L().Info("[DEBUG] Step resumed by client", logger.String("step_id", stepID))
	} else {
		// Headless/test mode simulator
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
			logger.L().Info("[DEBUG] Step auto-resumed (headless)", logger.String("step_id", stepID))
		}
	}

	// 3. Find worker and dispatch
	workerStream := o.registry.FindWorkerByCapability(capability)
	if workerStream == nil {
		return fmt.Errorf("no worker found for capability: %s", capability)
	}

	future := models.NewTaskFuture()
	workerStream.RegisterResultFuture(stepID, future)
	defer workerStream.DeregisterResultFuture(stepID)

	execTask := models.StepExecutionTask{
		WorkflowID:     workflowID,
		TaskID:         stepID,
		PreviousTaskID: prevTaskID,
		Step:           step,
		Resources:      orchestrator.ResolveResources(prog, step),
	}
	body, err := json.Marshal(execTask)
	if err != nil {
		return fmt.Errorf("failed to marshal step: %w", err)
	}
	if err := workerStream.Send(&transport.FlightData{DataBody: body}); err != nil {
		return fmt.Errorf("failed to send step to worker %s: %w", workerStream.GetID(), err)
	}

	// 4. Wait for result
	var stepErr error
	var taskRes models.TaskResult
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	taskRes, err = future.Await(timeoutCtx)
	if err != nil {
		if err == context.DeadlineExceeded {
			stepErr = fmt.Errorf("step %s timed out", stepID)
		} else {
			stepErr = err
		}
	} else if taskRes.Status != models.TaskStatusSuccess {
		stepErr = fmt.Errorf("step %s failed: %s", stepID, taskRes.ErrorMessage)
	}

	if stepErr != nil {
		if clientStream != nil {
			_ = clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Step %s failed: %v", stepID, stepErr)})
		}
		return stepErr
	}

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
				outputPreviews[fieldName] = fmt.Sprintf("<error formatting preview: %v>", err)
			}
		}
	}
	outputsJSON, _ := json.Marshal(outputPreviews)

	// 7. Report step completion with outputs
	if clientStream != nil {
		completeHeader := fmt.Sprintf("DEBUG_STEP_COMPLETE:%s:%s:%s", stepID, taskRes.Status, string(outputsJSON))
		if err := clientStream.Send(&transport.FlightData{DataBody: []byte(completeHeader)}); err != nil {
			return fmt.Errorf("failed to send debug completion to client: %w", err)
		}
	}

	// 8. Recurse to next steps
	for _, nextID := range step.Next {
		if err := o.executeStepDebug(ctx, workflowID, prog, nextID, stepID, schemas, clientStream, allOutputs, mu); err != nil {
			return err
		}
	}

	return nil
}
