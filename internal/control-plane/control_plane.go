package control_plane

import (
	"encoding/json"
	"fmt"
	"sync"

	"net"
	"os"
	"strings"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator"
	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator/graph"
	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator/interactive"
	"github.com/galgotech/heddle-lang/internal/control-plane/orchestrator/recursive"
	"github.com/galgotech/heddle-lang/internal/control-plane/registry"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

// ControlPlaneServer orchestrates high-performance task execution DAGs, manages worker
// lifecycle and capability routing, and exposes the Arrow Flight RPC service endpoints.
type ControlPlaneServer struct {
	flight.BaseFlightServer
	registry *registry.WorkerRegistry

	mu            sync.Mutex
	Ready         chan struct{}
	orchestrators map[orchestrator.Strategy]orchestrator.Orchestrator
}

// DoAction processes control plane administrative actions sent via unary Flight action requests.
func (s *ControlPlaneServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	ctx := stream.Context()
	metaData, _ := metadata.FromIncomingContext(ctx)

	workerID := ""
	if ids := metaData.Get("worker-id"); len(ids) > 0 {
		workerID = ids[0]
	}

	// Validate presence of worker-id for all actions except initial workflow submissions.
	if action.Type != models.ActionSubmitWorkflow {
		if workerID == "" {
			return status.Error(codes.Unauthenticated, "missing worker-id")
		}
	}

	switch action.Type {
	case models.ActionRegisterWorker:
		// Decode registration details to dynamically register a new worker instance.
		var reg models.WorkerRegistration
		if err := json.Unmarshal(action.Body, &reg); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal registration: %v", err)
		}

		s.registry.Register(workerID, reg)
		logger.L().Info("Worker registered", zap.String("id", workerID), zap.String("address", reg.Address))
		return stream.Send(&flight.Result{Body: []byte("OK")})

	case models.ActionHeartbeat:
		// Refresh the worker's active status and update current execution load telemetry.
		var hb models.WorkerHeartbeat
		if err := json.Unmarshal(action.Body, &hb); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal heartbeat: %v", err)
		}
		if ok := s.registry.Heartbeat(workerID, hb.Load); !ok {
			return status.Errorf(codes.NotFound, "worker %s not registered", workerID)
		}
		return stream.Send(&flight.Result{Body: []byte("OK")})

	case models.ActionUpdateCapabilities:
		// Update capabilities and register structural schemas for steps executed by this worker.
		var update models.WorkerCapabilitiesUpdate
		if err := json.Unmarshal(action.Body, &update); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal capabilities update: %v", err)
		}
		if ok := s.registry.UpdateCapabilities(workerID, update); !ok {
			return status.Errorf(codes.NotFound, "worker %s not registered", workerID)
		}
		logger.L().Info("Worker capabilities updated", zap.String("id", workerID), zap.Strings("capabilities", update.Capabilities))
		return stream.Send(&flight.Result{Body: []byte("OK")})

	case models.ActionSubmitWorkflow:
		// Submit a new workflow run by compiling Heddle DSL source and queuing the parsed DAG task.
		var sub models.WorkflowSubmission
		if err := json.Unmarshal(action.Body, &sub); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal submission: %v", err)
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
			step, ok := inst.(*ir.StepInstruction)
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
			w := s.registry.FindWorkerStreamForStep(capability)
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
			Program:        program,
			TargetWorkflow: sub.WorkflowName,
			Schemas:        schemas,
		}

		strategy := orchestrator.Strategy(task.Strategy)
		orch, ok := s.orchestrators[strategy]
		if !ok {
			orch = s.orchestrators[orchestrator.StrategyRecursive]
		}
		go orch.OrchestrateTask(ctx, task)

		logger.L().Info("Workflow compiled and queued", zap.String("task_id", task.ID))
		return stream.Send(&flight.Result{Body: fmt.Appendf(nil, "QUEUED: %s", task.ID)})

	case models.ActionGetWorkerInfo:
		// Retrieve schema specifications for all registered step capabilities.
		info := s.registry.GetRegistryInfo()
		body, err := json.Marshal(info)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal registry info: %v", err)
		}
		return stream.Send(&flight.Result{Body: body})

	default:
		return status.Errorf(codes.Unimplemented, "action %s not implemented", action.Type)
	}
}

// DoExchange establishes persistent, bi-directional streams with workers for task coordination and results.
func (s *ControlPlaneServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	ctx := stream.Context()
	metaData, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(metaData.Get("worker-id")) == 0 {
		return status.Error(codes.Unauthenticated, "missing worker-id")
	}
	workerID := metaData.Get("worker-id")[0]

	s.registry.ProcessStream(workerID, stream)
	defer s.registry.StopStream(workerID)

	logger.L().Info("Worker connected", zap.String("id", workerID))

	return nil
}

// Listen starts the gRPC and Flight service listeners on the target address (handling TCP or Unix domain sockets).
func (s *ControlPlaneServer) Listen(addr string) error {
	var lis net.Listener
	var err error

	// Intercept unix socket schemes to cleanup pre-existing sockets on the filesystem before binding.
	if after, ok := strings.CutPrefix(addr, "unix://"); ok {
		path := after
		if _, err := os.Stat(path); err == nil {
			os.Remove(path)
		}
		lis, err = net.Listen("unix", path)
	} else {
		lis, err = net.Listen("tcp", addr)
	}

	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)

	logger.L().Info("Control Plane listening", zap.String("address", addr))

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(lis)
	}()

	// Signal server initialization completion to dynamic orchestrators or tests.
	if s.Ready != nil {
		close(s.Ready)
		s.Ready = nil
	}

	return <-errCh
}

// NewControlPlaneServer instantiates the control plane server and registers supported scheduling orchestrator strategies.
func NewControlPlaneServer(registry *registry.WorkerRegistry) *ControlPlaneServer {
	s := &ControlPlaneServer{
		registry: registry,
		Ready:    make(chan struct{}),
	}
	s.orchestrators = map[orchestrator.Strategy]orchestrator.Orchestrator{
		orchestrator.StrategyRecursive:   recursive.NewRecursiveOrchestrator(registry),
		orchestrator.StrategyGraph:       graph.NewGraphOrchestrator(registry),
		orchestrator.StrategyInteractive: interactive.NewInteractiveOrchestrator(registry),
	}
	return s
}
