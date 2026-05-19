package interactive

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

type InteractiveOrchestrator struct {
	registry *registry.WorkerRegistry
}

func (o *InteractiveOrchestrator) OrchestrateTask(ctx context.Context, task models.Task) {
	program := task.Program
	for _, flowID := range program.Workflows {
		flow := program.Instructions[flowID].(*ir.FlowInstruction)

		// Filter by workflow name if specified
		if task.TargetWorkflow != "" && flow.Name != task.TargetWorkflow {
			continue
		}

		logger.L().Info("[INTERACTIVE] Starting interactive workflow execution", zap.String("workflow", flow.Name))

		for _, headID := range flow.Heads {
			if err := o.executeStepInteractive(ctx, task.ID, program, headID, "", task.Schemas); err != nil {
				logger.L().Error("[INTERACTIVE] Task failed", zap.Error(err))
				return
			}
		}
	}
	logger.L().Info("[INTERACTIVE] Task completed successfully", zap.String("id", task.ID))
}

func (o *InteractiveOrchestrator) executeStepInteractive(ctx context.Context, workflowID string, prog *ir.Program, stepID string, prevTaskID string, schemas map[string]schema.StepSchemas) error {
	// 0. Validate Schema Compatibility
	if err := orchestrator.ValidateEdge(prog, prevTaskID, stepID, schemas); err != nil {
		return err
	}

	step := prog.Instructions[stepID].(*ir.StepInstruction)
	capability := fmt.Sprintf("%s.%s", step.Call[0], step.Call[1])

	logger.L().Info("[INTERACTIVE] Prompting approval for step", zap.String("step_id", stepID), zap.String("capability", capability))

	// Simulate interactive step gate / approval latency
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Millisecond):
		logger.L().Info("[INTERACTIVE] Step approved for execution", zap.String("step_id", stepID))
	}

	// 1. Find worker
	worker := o.registry.FindWorkerStreamForStep(capability)
	if worker == nil {
		return fmt.Errorf("no worker found for capability: %s", capability)
	}

	// 2. Get worker stream
	workerStream, ok := o.registry.GetActiveStream(worker.GetID())
	if !ok {
		return fmt.Errorf("worker %s stream not found", worker.GetID())
	}

	// 3. Create result channel and register it
	resultCh := make(chan models.TaskResult, 1)

	// 4. Dispatch step
	execTask := models.StepExecutionTask{
		WorkflowID:     workflowID,
		TaskID:         stepID,
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
			return fmt.Errorf("step %s failed: %s", stepID, res.ErrorMessage)
		}
	case <-time.After(30 * time.Second):
		return fmt.Errorf("step %s timed out", stepID)
	}

	// 6. Continue to next steps
	for _, nextID := range step.Next {
		if err := o.executeStepInteractive(ctx, workflowID, prog, nextID, stepID, schemas); err != nil {
			return err
		}
	}

	return nil
}

func NewInteractiveOrchestrator(registry *registry.WorkerRegistry) *InteractiveOrchestrator {
	return &InteractiveOrchestrator{registry: registry}
}
