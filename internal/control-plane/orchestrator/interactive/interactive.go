package interactive

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

type InteractiveOrchestrator struct {
	registry *registry.WorkerRegistry
}

func (o *InteractiveOrchestrator) OrchestrateTask(ctx context.Context, task models.Task) {
	if task.ClientID != "" {
		o.registry.RegisterWorkflowClient(task.ID, task.ClientID)
		defer o.registry.DeregisterWorkflowClient(task.ID)
	}

	program := task.Program
	clientStream, _ := o.registry.GetActiveClientStream(task.ClientID)

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

		logger.L().Info("[INTERACTIVE] Starting interactive workflow execution", logger.String("workflow", flow.Name))
		if clientStream != nil {
			clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Starting interactive execution of workflow %s...", flow.Name)})
		}

		var runErr error
		for _, headID := range flow.Heads {
			if err := o.executeStepInteractive(ctx, task.ID, program, headID, "", task.Schemas, clientStream); err != nil {
				runErr = err
				break
			}
		}

		if runErr != nil {
			logger.L().Error("[INTERACTIVE] Task failed", logger.Error(runErr))
			if clientStream != nil {
				clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Workflow failed: %v", runErr)})
			}
			return
		}
	}
	logger.L().Info("[INTERACTIVE] Task completed successfully", logger.String("id", task.ID))
	if clientStream != nil {
		clientStream.Send(&transport.FlightData{DataBody: []byte("LOG:Workflow completed successfully.")})
	}
}

func (o *InteractiveOrchestrator) executeStepInteractive(ctx context.Context, workflowID string, prog ir.Program, stepID string, prevTaskID string, schemas map[string]schema.StepSchemas, clientStream transport.ExchangeStream) error {
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

	logger.L().Info("[INTERACTIVE] Prompting approval for step", logger.String("step_id", stepID), logger.String("capability", capability))

	if clientStream != nil {
		err := clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "PROMPT:%s:%s", stepID, capability)})
		if err != nil {
			return fmt.Errorf("failed to send interactive prompt to client: %w", err)
		}

		msg, err := clientStream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive interactive response from client: %w", err)
		}

		if string(msg.DataBody) != "APPROVE" {
			return fmt.Errorf("step execution rejected by user")
		}
		logger.L().Info("[INTERACTIVE] Step approved for execution", logger.String("step_id", stepID))
	} else {
		// Simulate interactive step gate / approval latency when clientStream is nil (headless or mock environments)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
			logger.L().Info("[INTERACTIVE] Step approved for execution (headless)", logger.String("step_id", stepID))
		}
	}

	if clientStream != nil {
		clientStream.Send(&transport.FlightData{DataBody: fmt.Appendf(nil, "LOG:Executing step %s (%s)...", stepID, capability)})
	}

	// 1. Find worker
	worker := o.registry.FindWorkerStreamForStep(capability)
	if worker == nil {
		return fmt.Errorf("no worker found for capability: %s", capability)
	}

	// 2. Get worker stream
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
		if err := o.executeStepInteractive(ctx, workflowID, prog, nextID, stepID, schemas, clientStream); err != nil {
			return err
		}
	}

	return nil
}

func NewInteractiveOrchestrator(registry *registry.WorkerRegistry) *InteractiveOrchestrator {
	return &InteractiveOrchestrator{registry: registry}
}
