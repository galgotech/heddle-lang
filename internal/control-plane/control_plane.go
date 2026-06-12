package control_plane

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator"
	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator/debug"
	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator/graph"
	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator/interactive"
	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator/recursive"
	"github.com/galgotech/heddle-lang/internal/control-plane/registry"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/schema"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

// ControlPlaneServer orchestrates high-performance task execution DAGs, manages worker
// lifecycle and capability routing, and implements the transport.Server interface.
type ControlPlaneServer struct {
	registry      *registry.NodeRegistry
	orchestrators map[orchestrator.Strategy]orchestrator.Orchestrator
}

// NewControlPlaneServer instantiates the control plane server and registers supported scheduling orchestrator strategies.
func NewControlPlaneServer(registry *registry.NodeRegistry) *ControlPlaneServer {
	s := &ControlPlaneServer{
		registry: registry,
		orchestrators: map[orchestrator.Strategy]orchestrator.Orchestrator{
			orchestrator.StrategyRecursive:   recursive.NewRecursiveOrchestrator(registry),
			orchestrator.StrategyGraph:       graph.NewGraphOrchestrator(registry),
			orchestrator.StrategyInteractive: interactive.NewInteractiveOrchestrator(registry),
			orchestrator.StrategyDebug:       debug.NewDebugOrchestrator(registry),
		},
	}
	return s
}

// DoAction processes control plane administrative actions sent via unary action requests.
func (s *ControlPlaneServer) DoAction(ctx context.Context, action *transport.Action, stream transport.ServerStream) error {
	metaData, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}

	var nodeID string
	var nodeType registry.NodeType

	if ids := metaData.Get("worker-id"); len(ids) > 0 {
		nodeID = ids[0]
		nodeType = registry.NodeTypeWorker
	} else if ids := metaData.Get("client-id"); len(ids) > 0 {
		nodeID = ids[0]
		nodeType = registry.NodeTypeClient
	} else {
		return status.Error(codes.Unauthenticated, "missing worker-id or client-id")
	}

	switch action.Type {
	case models.ActionRegisterWorker:
		// Decode registration details to dynamically register a new worker instance.
		var reg models.WorkerRegistration
		if err := json.Unmarshal(action.Body, &reg); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal registration: %v", err)
		}

		s.registry.RegisterNode(nodeID, registry.NodeTypeWorker, reg)
		logger.L().Info("Worker registered", logger.String("id", nodeID), logger.String("address", reg.Address))
		return stream.Send(&transport.Result{Body: []byte("OK")})

	case models.ActionDeregisterWorker:
		s.registry.DeregisterNode(nodeID)
		logger.L().Info("Worker deregistered", logger.String("id", nodeID))
		return stream.Send(&transport.Result{Body: []byte("OK")})

	case models.ActionRegisterClient:
		s.registry.RegisterNode(nodeID, registry.NodeTypeClient, models.WorkerRegistration{})
		logger.L().Info("Client registered", logger.String("id", nodeID))
		return stream.Send(&transport.Result{Body: []byte("OK")})

	case models.ActionDeregisterClient:
		s.registry.DeregisterNode(nodeID)
		logger.L().Info("Client deregistered", logger.String("id", nodeID))
		return stream.Send(&transport.Result{Body: []byte("OK")})

	case models.ActionHeartbeat:
		// Refresh the node's active status and update current execution load telemetry.
		var hb models.WorkerHeartbeat
		if err := json.Unmarshal(action.Body, &hb); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal heartbeat: %v", err)
		}
		if ok := s.registry.Heartbeat(nodeID, hb.Load); !ok {
			return status.Errorf(codes.NotFound, "%s %s not registered", nodeType, nodeID)
		}
		return stream.Send(&transport.Result{Body: []byte("OK")})

	case models.ActionUpdateCapabilities:
		// Update capabilities and register structural schemas for steps executed by this worker.
		var update models.WorkerCapabilitiesUpdate
		if err := json.Unmarshal(action.Body, &update); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal capabilities update: %v", err)
		}
		if ok := s.registry.UpdateCapabilities(nodeID, update); !ok {
			return status.Errorf(codes.NotFound, "worker %s not registered", nodeID)
		}
		logger.L().Info("Worker capabilities updated", logger.String("id", nodeID), logger.Strings("capabilities", update.Capabilities))
		return stream.Send(&transport.Result{Body: []byte("OK")})

	case models.ActionSubmitWorkflow:
		// Submit a new workflow run by compiling Heddle DSL source and queuing the parsed DAG task.
		var sub models.WorkflowSubmission
		if err := json.Unmarshal(action.Body, &sub); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal submission: %v", err)
		}

		if sub.Strategy == string(orchestrator.StrategyDebug) && sub.Async {
			return status.Error(codes.InvalidArgument, "debug execution strategy cannot be executed asynchronously")
		}

		logger.L().Info("Received workflow submission")

		// 1. Compile the DSL source into an executable intermediate representation (IR) program.
		c := compiler.New()
		program, err := c.Compile(sub.Source)
		if err != nil {
			return status.Errorf(codes.Internal, "compilation failed: %v", err)
		}

		// Validate compatibility by verifying that all required capabilities are registered.
		schemas := make(map[string]schema.StepSchemas)
		for _, inst := range program.Instructions {
			step, ok := inst.(ir.StepInstruction)
			if !ok {
				continue
			}
			if len(step.Call) < 2 {
				return status.Errorf(codes.FailedPrecondition, "validation failed: invalid call format for step %q", step.ID)
			}
			if step.Call[0] == "__internal" {
				continue
			}
			capability := fmt.Sprintf("%s.%s", step.Call[0], step.Call[1])
			w := s.registry.FindWorkerByCapability(capability)
			if w == nil {
				return status.Errorf(codes.FailedPrecondition, "validation failed: no worker registered for capability %q", capability)
			}
			if s, ok := w.GetSchemaForCapability(capability); ok {
				schemas[capability] = s
			}
		}

		// 2. Queue the task with its compiled program structure and step validation schemas.
		task := models.Task{
			ID:             uuid.New().String(),
			ClientID:       nodeID,
			Program:        program,
			TargetWorkflow: sub.WorkflowName,
			Strategy:       sub.Strategy,
			Schemas:        schemas,
		}

		strategy := orchestrator.Strategy(task.Strategy)
		orch, ok := s.orchestrators[strategy]
		if !ok {
			orch = s.orchestrators[orchestrator.StrategyRecursive]
		}
		go orch.OrchestrateTask(context.WithoutCancel(ctx), task)

		logger.L().Info("Workflow compiled and queued", logger.String("client_id", nodeID), logger.String("task_id", task.ID))
		return stream.Send(&transport.Result{Body: fmt.Appendf(nil, "QUEUED: %s", task.ID)})

	case models.ActionGetWorkerInfo:
		// Retrieve schema specifications for all registered step capabilities.
		info := s.registry.GetRegistryInfo()
		body, err := json.Marshal(info)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal registry info: %v", err)
		}
		return stream.Send(&transport.Result{Body: body})

	default:
		return status.Errorf(codes.Unimplemented, "action %s not implemented", action.Type)
	}
}

// DoExchange establishes persistent, bi-directional streams with workers and clients for task coordination and results.
func (s *ControlPlaneServer) DoExchange(ctx context.Context, stream transport.ExchangeStream) error {
	metaData, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}

	var nodeID string
	var nodeType registry.NodeType

	if workerIDs := metaData.Get("worker-id"); len(workerIDs) > 0 {
		nodeID = workerIDs[0]
		nodeType = registry.NodeTypeWorker
	} else if clientIDs := metaData.Get("client-id"); len(clientIDs) > 0 {
		nodeID = clientIDs[0]
		nodeType = registry.NodeTypeClient
	} else {
		return status.Error(codes.Unauthenticated, "missing worker-id or client-id")
	}

	node, ok := s.registry.GetNode(nodeID)
	if !ok {
		return status.Errorf(codes.NotFound, "%s %s not registered", nodeType, nodeID)
	}

	logger.L().Info(fmt.Sprintf("Node %s connected", nodeType), logger.String("id", nodeID))

	errChan := node.ProcessStream(stream)
	defer func() {
		s.registry.DeregisterNode(nodeID)
		logger.L().Info(fmt.Sprintf("Node %s disconnected and deregistered", nodeType), logger.String("id", nodeID))
	}()

	select {
	case err := <-errChan:
		if err == io.EOF {
			return nil
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
