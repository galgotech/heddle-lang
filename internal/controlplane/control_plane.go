package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/galgotech/heddle-lang/internal/controlplane/orchestrator"
	"github.com/galgotech/heddle-lang/internal/controlplane/orchestrator/debug"
	"github.com/galgotech/heddle-lang/internal/controlplane/orchestrator/graph"
	"github.com/galgotech/heddle-lang/internal/controlplane/orchestrator/interactive"
	"github.com/galgotech/heddle-lang/internal/controlplane/orchestrator/recursive"
	"github.com/galgotech/heddle-lang/internal/controlplane/registry"
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
	logger.L().Debug("control plane initialized: registered scheduling orchestrator strategies", logger.Component("control-plane"))
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
		logger.L().Warn("incoming request failed: missing context metadata", logger.Component("control-plane"))
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
		logger.L().Warn("incoming request failed: missing node identifier in metadata", logger.Component("control-plane"))
		return status.Error(codes.Unauthenticated, "missing worker-id or client-id")
	}

	var nodeField logger.Field
	if nodeType == registry.NodeTypeWorker {
		nodeField = logger.WorkerID(nodeID)
	} else {
		nodeField = logger.ClientID(nodeID)
	}

	logger.L().Debug("action received: processing control plane command",
		logger.Component("control-plane"),
		nodeField,
		logger.String("action_type", action.Type),
	)

	switch action.Type {
	case models.ActionRegisterWorker:
		// Decode registration details to dynamically register a new worker instance.
		var reg models.WorkerRegistration
		if err := json.Unmarshal(action.Body, &reg); err != nil {
			logger.L().Error("worker registration failed: unable to decode body",
				logger.Component("control-plane"),
				logger.WorkerID(nodeID),
				logger.Error(err),
			)
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal registration: %v", err)
		}

		s.registry.RegisterNode(nodeID, registry.NodeTypeWorker, reg)
		logger.L().Info("worker registered: successfully registered worker node",
			logger.Component("control-plane"),
			logger.WorkerID(nodeID),
			logger.String("address", reg.Address),
		)
		if err := stream.Send(&transport.Result{Body: []byte("OK")}); err != nil {
			logger.L().Error("worker registration failed: unable to send confirmation result",
				logger.Component("control-plane"),
				logger.WorkerID(nodeID),
				logger.Error(err),
			)
			return err
		}
		return nil

	case models.ActionDeregisterWorker:
		s.registry.DeregisterNode(nodeID)
		logger.L().Info("worker deregistered: successfully removed worker node",
			logger.Component("control-plane"),
			logger.WorkerID(nodeID),
		)
		if err := stream.Send(&transport.Result{Body: []byte("OK")}); err != nil {
			logger.L().Error("worker deregistration failed: unable to send confirmation result",
				logger.Component("control-plane"),
				logger.WorkerID(nodeID),
				logger.Error(err),
			)
			return err
		}
		return nil

	case models.ActionRegisterClient:
		s.registry.RegisterNode(nodeID, registry.NodeTypeClient, models.WorkerRegistration{})
		logger.L().Info("client registered: successfully registered client node",
			logger.Component("control-plane"),
			logger.ClientID(nodeID),
		)
		if err := stream.Send(&transport.Result{Body: []byte("OK")}); err != nil {
			logger.L().Error("client registration failed: unable to send confirmation result",
				logger.Component("control-plane"),
				logger.ClientID(nodeID),
				logger.Error(err),
			)
			return err
		}
		return nil

	case models.ActionDeregisterClient:
		s.registry.DeregisterNode(nodeID)
		logger.L().Info("client deregistered: successfully removed client node",
			logger.Component("control-plane"),
			logger.ClientID(nodeID),
		)
		if err := stream.Send(&transport.Result{Body: []byte("OK")}); err != nil {
			logger.L().Error("client deregistration failed: unable to send confirmation result",
				logger.Component("control-plane"),
				logger.ClientID(nodeID),
				logger.Error(err),
			)
			return err
		}
		return nil

	case models.ActionHeartbeat:
		// Refresh the node's active status and update current execution load telemetry.
		var hb models.WorkerHeartbeat
		if err := json.Unmarshal(action.Body, &hb); err != nil {
			logger.L().Error("heartbeat failed: unable to decode body",
				logger.Component("control-plane"),
				nodeField,
				logger.Error(err),
			)
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal heartbeat: %v", err)
		}
		if ok := s.registry.Heartbeat(nodeID, hb.Load); !ok {
			logger.L().Warn("heartbeat failed: node not registered in registry",
				logger.Component("control-plane"),
				nodeField,
			)
			return status.Errorf(codes.NotFound, "%s %s not registered", nodeType, nodeID)
		}
		logger.L().Debug("heartbeat processed: successfully updated node active status",
			logger.Component("control-plane"),
			nodeField,
			logger.Int("load", hb.Load),
		)
		if err := stream.Send(&transport.Result{Body: []byte("OK")}); err != nil {
			logger.L().Error("heartbeat processing failed: unable to send confirmation result",
				logger.Component("control-plane"),
				nodeField,
				logger.Error(err),
			)
			return err
		}
		return nil

	case models.ActionUpdateCapabilities:
		// Update capabilities and register structural schemas for steps executed by this worker.
		var update models.WorkerCapabilitiesUpdate
		if err := json.Unmarshal(action.Body, &update); err != nil {
			logger.L().Error("capabilities update failed: unable to decode body",
				logger.Component("control-plane"),
				logger.WorkerID(nodeID),
				logger.Error(err),
			)
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal capabilities update: %v", err)
		}
		if ok := s.registry.UpdateCapabilities(nodeID, update); !ok {
			logger.L().Warn("capabilities update failed: worker not registered",
				logger.Component("control-plane"),
				logger.WorkerID(nodeID),
			)
			return status.Errorf(codes.NotFound, "worker %s not registered", nodeID)
		}
		logger.L().Info("worker capabilities updated: registered schemas for worker",
			logger.Component("control-plane"),
			logger.WorkerID(nodeID),
			logger.Strings("capabilities", update.Capabilities),
		)
		if err := stream.Send(&transport.Result{Body: []byte("OK")}); err != nil {
			logger.L().Error("capabilities update failed: unable to send confirmation result",
				logger.Component("control-plane"),
				logger.WorkerID(nodeID),
				logger.Error(err),
			)
			return err
		}
		return nil

	case models.ActionSubmitWorkflow:
		// Submit a new workflow run by compiling Heddle DSL source and queuing the parsed DAG task.
		var sub models.WorkflowSubmission
		if err := json.Unmarshal(action.Body, &sub); err != nil {
			logger.L().Error("workflow submission failed: unable to decode body",
				logger.Component("control-plane"),
				logger.ClientID(nodeID),
				logger.Error(err),
			)
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal submission: %v", err)
		}

		if sub.Strategy == string(orchestrator.StrategyDebug) && sub.Async {
			logger.L().Warn("workflow submission failed: debug strategy cannot be run asynchronously",
				logger.Component("control-plane"),
				logger.ClientID(nodeID),
			)
			return status.Error(codes.InvalidArgument, "debug execution strategy cannot be executed asynchronously")
		}

		logger.L().Info("workflow submission received: processing new workflow run request",
			logger.Component("control-plane"),
			logger.ClientID(nodeID),
		)

		// 1. Compile the DSL source into an executable intermediate representation (IR) program.
		c := compiler.New()
		program, err := c.Compile(sub.Source)
		if err != nil {
			logger.L().Error("workflow compilation failed: dsl compile error",
				logger.Component("control-plane"),
				logger.ClientID(nodeID),
				logger.Error(err),
			)
			return status.Errorf(codes.Internal, "compilation failed: %v", err)
		}

		logger.L().Debug("workflow compiled successfully: verifying step capabilities",
			logger.Component("control-plane"),
			logger.ClientID(nodeID),
		)

		// Validate compatibility by verifying that all required capabilities are registered.
		schemas := make(map[string]schema.StepSchemas)
		for _, inst := range program.Instructions {
			step, ok := inst.(ir.StepInstruction)
			if !ok {
				continue
			}
			if len(step.Call) < 2 {
				err := fmt.Errorf("invalid call format for step %q", step.ID)
				logger.L().Error("workflow validation failed: step format error",
					logger.Component("control-plane"),
					logger.ClientID(nodeID),
					logger.Error(err),
				)
				return status.Errorf(codes.FailedPrecondition, "validation failed: invalid call format for step %q", step.ID)
			}
			if step.Call[0] == "__internal" {
				continue
			}
			capability := fmt.Sprintf("%s.%s", step.Call[0], step.Call[1])
			w := s.registry.FindWorkerByCapability(capability)
			if w == nil {
				err := fmt.Errorf("no worker registered for capability %q", capability)
				logger.L().Warn("workflow validation failed: capability worker not found",
					logger.Component("control-plane"),
					logger.ClientID(nodeID),
					logger.Capability(capability),
					logger.Error(err),
				)
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
			logger.L().Warn("workflow strategy resolution: strategy not found, falling back to recursive",
				logger.Component("control-plane"),
				logger.ClientID(nodeID),
				logger.TraceID(task.ID),
				logger.String("strategy", task.Strategy),
			)
			orch = s.orchestrators[orchestrator.StrategyRecursive]
		}

		logger.L().Debug("workflow orchestrator selected: initiating task execution",
			logger.Component("control-plane"),
			logger.ClientID(nodeID),
			logger.TraceID(task.ID),
			logger.String("strategy", string(strategy)),
		)

		go orch.OrchestrateTask(context.WithoutCancel(ctx), task)

		logger.L().Info("workflow compiled and queued: task orchestration started",
			logger.Component("control-plane"),
			logger.ClientID(nodeID),
			logger.TraceID(task.ID),
		)
		if err := stream.Send(&transport.Result{Body: fmt.Appendf(nil, "QUEUED: %s", task.ID)}); err != nil {
			logger.L().Error("workflow submission failed: unable to send queue result to client",
				logger.Component("control-plane"),
				logger.ClientID(nodeID),
				logger.TraceID(task.ID),
				logger.Error(err),
			)
			return err
		}
		return nil

	case models.ActionGetWorkerInfo:
		// Retrieve schema specifications for all registered step capabilities.
		info := s.registry.GetRegistryInfo()
		body, err := json.Marshal(info)
		if err != nil {
			logger.L().Error("worker info retrieval failed: unable to marshal registry info",
				logger.Component("control-plane"),
				nodeField,
				logger.Error(err),
			)
			return status.Errorf(codes.Internal, "failed to marshal registry info: %v", err)
		}
		logger.L().Debug("worker info retrieved: successfully returned registry info",
			logger.Component("control-plane"),
			nodeField,
		)
		if err := stream.Send(&transport.Result{Body: body}); err != nil {
			logger.L().Error("worker info retrieval failed: unable to send registry info",
				logger.Component("control-plane"),
				nodeField,
				logger.Error(err),
			)
			return err
		}
		return nil

	default:
		logger.L().Warn("incoming request failed: action not implemented",
			logger.Component("control-plane"),
			nodeField,
			logger.String("action_type", action.Type),
		)
		return status.Errorf(codes.Unimplemented, "action %s not implemented", action.Type)
	}
}

// DoExchange establishes persistent, bi-directional streams with workers and clients for task coordination and results.
func (s *ControlPlaneServer) DoExchange(ctx context.Context, stream transport.ExchangeStream) error {
	metaData, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		logger.L().Warn("exchange setup failed: missing context metadata", logger.Component("control-plane"))
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
		logger.L().Warn("exchange setup failed: missing node identifier in metadata", logger.Component("control-plane"))
		return status.Error(codes.Unauthenticated, "missing worker-id or client-id")
	}

	node, ok := s.registry.GetNode(nodeID)
	if !ok {
		logger.L().Warn("exchange connection rejected: node not found in registry",
			logger.Component("control-plane"),
			logger.String("node_type", string(nodeType)),
			logger.String("node_id", nodeID),
		)
		return status.Errorf(codes.NotFound, "%s %s not registered", nodeType, nodeID)
	}

	var idField logger.Field
	if nodeType == registry.NodeTypeWorker {
		idField = logger.WorkerID(nodeID)
	} else {
		idField = logger.ClientID(nodeID)
	}
	logger.L().Info(fmt.Sprintf("node connected: %s established bidirectional stream", nodeType),
		logger.Component("control-plane"),
		idField,
	)

	errChan := node.ProcessStream(stream)

	defer func() {
		s.registry.DeregisterNode(nodeID)
		logger.L().Info(fmt.Sprintf("node disconnected: %s closed stream and was deregistered", nodeType),
			logger.Component("control-plane"),
			idField,
		)
	}()

	select {
	case err := <-errChan:
		if err == io.EOF {
			logger.L().Debug(fmt.Sprintf("stream terminated: %s disconnected gracefully", nodeType),
				logger.Component("control-plane"),
				idField,
			)
			return nil
		}
		if isCanceledError(err) {
			logger.L().Debug(fmt.Sprintf("stream terminated: %s connection canceled", nodeType),
				logger.Component("control-plane"),
				idField,
				logger.Error(err),
			)
			return err
		}
		logger.L().Error(fmt.Sprintf("stream terminated with error: %s connection closed abruptly", nodeType),
			logger.Component("control-plane"),
			idField,
			logger.Error(err),
		)
		return err
	case <-ctx.Done():
		logger.L().Debug(fmt.Sprintf("stream terminated: context cancelled for %s connection", nodeType),
			logger.Component("control-plane"),
			idField,
		)
		return ctx.Err()
	}
}

func isCanceledError(err error) bool {
	if err == nil {
		return false
	}
	if err == context.Canceled {
		return true
	}
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.Canceled
	}
	return false
}
