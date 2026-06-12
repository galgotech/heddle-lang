package recursive

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator"
	"github.com/galgotech/heddle-lang/internal/control-plane/registry"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/schema"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

type RecursiveOrchestrator struct {
	registry *registry.WorkerRegistry
}

func (o *RecursiveOrchestrator) OrchestrateTask(ctx context.Context, task models.Task) {
	if task.ClientID == "" {
		logger.L().Error("Task does not have a defined ClientID")
		return
	}

	o.registry.RegisterWorkflowClient(task.ID, task.ClientID)
	defer o.registry.DeregisterWorkflowClient(task.ID)

	program := task.Program
	clientStream, _ := o.registry.GetActiveClientStream(task.ClientID)

	if clientStream == nil {
		logger.L().Warn("Stream will not be sent to client", logger.String("client_id", task.ClientID))
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
			logger.L().Error("flow is not a valid FlowInstruction", logger.String("id", flowID))
			continue
		}

		// Filter by workflow name if specified
		if task.TargetWorkflow != "" && flow.Name != task.TargetWorkflow {
			continue
		}

		if clientStream != nil {
			clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Starting execution of workflow %s...", flow.Name)})
		}

		var runErr error
		for _, headID := range flow.Heads {
			if err := o.executeStepRecursive(ctx, task.ID, program, headID, "", task.Schemas, clientStream); err != nil {
				runErr = err
				break
			}
		}

		if runErr != nil {
			logger.L().Error("Task failed", logger.Error(runErr))
			if clientStream != nil {
				clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Workflow failed: %v", runErr)})
			}
			return
		}
	}

	logger.L().Info("Task completed successfully", logger.String("id", task.ID))
	if clientStream != nil {
		clientStream.Send(&transport.FlightData{DataBody: []byte("LOG:Workflow completed successfully.")})
	}
}

func (o *RecursiveOrchestrator) executeStepRecursive(ctx context.Context, workflowID string, prog ir.Program, stepID string, prevTaskID string, schemas map[string]schema.StepSchemas, clientStream transport.ExchangeStream) error {
	// 0. Validate Schema Compatibility
	if err := orchestrator.ValidateEdge(prog, prevTaskID, stepID, schemas); err != nil {
		if clientStream != nil {
			clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Validation failed for step %s: %v", stepID, err)})
		}
		return err
	}

	step, ok := prog.Instructions[stepID].(ir.StepInstruction)
	if !ok {
		return fmt.Errorf("step %s is not a valid instruction", stepID)
	}
	capability := fmt.Sprintf("%s.%s", step.Call[0], step.Call[1])

	if clientStream != nil {
		clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Executing step %s (%s)...", stepID, capability)})
	}

	// 1. Find worker
	worker := o.registry.FindWorkerByCapability(capability)
	if worker == nil {
		return fmt.Errorf("no worker found for capability: %s", capability)
	}

	workerStream, ok := o.registry.GetActiveWorkerStream(worker.GetID())
	if !ok {
		return fmt.Errorf("worker %s stream not found", worker.GetID())
	}

	// 3. Create result channel and register it
	resultCh := make(chan models.TaskResult, 1)
	o.registry.RegisterResultChan(stepID, resultCh)
	defer o.registry.DeregisterResultChan(stepID)

	// 4. Dispatch step
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
		return fmt.Errorf("failed to send step to worker %s: %w", worker.GetID(), err)
	}

	// 5. Wait for result
	var stepErr error
	select {
	case <-ctx.Done():
		stepErr = ctx.Err()
	case res := <-resultCh:
		if res.Status != models.TaskStatusSuccess {
			stepErr = fmt.Errorf("step %s failed: %s", stepID, res.ErrorMessage)
		}
	case <-time.After(30 * time.Second):
		stepErr = fmt.Errorf("step %s timed out", stepID)
	}

	if stepErr != nil {
		if clientStream != nil {
			clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Step %s failed: %v", stepID, stepErr)})
		}
		return stepErr
	}

	if clientStream != nil {
		clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Step %s completed successfully.", stepID)})
	}

	// 6. Continue to next steps
	for _, nextID := range step.Next {
		if err := o.executeStepRecursive(ctx, workflowID, prog, nextID, stepID, schemas, clientStream); err != nil {
			return err
		}
	}

	return nil
}

func NewRecursiveOrchestrator(registry *registry.WorkerRegistry) *RecursiveOrchestrator {
	return &RecursiveOrchestrator{registry: registry}
}
