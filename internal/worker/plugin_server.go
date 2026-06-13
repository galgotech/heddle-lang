package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"sync"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

type pluginSdk interface {
	PluginRegistration() plugin.PluginRegistration
	Stream(stream flight.FlightService_DoExchangeServer)
	HaveStream() bool
	Send(ctx context.Context, request plugin.ExecuteStepRequest) error
	Recv() (plugin.ExecuteStepResponse, error)
	LastHeartbeat(hb plugin.Heartbeat)
}

type PluginServer struct {
	flight.BaseFlightServer
	registry           *locality.DataLocalityRegistry
	pluginSyncRegister chan plugin.PluginRegistration

	socketPath    string
	nativePlugins []pluginSdk
	plugins       map[string]pluginSdk // map[string]*PluginInfo
	pluginsMU     sync.RWMutex
	Ready         chan struct{}
}

func NewPluginServer(registry *locality.DataLocalityRegistry, nativePlugins []pluginSdk, socketPath string) *PluginServer {
	pluginServer := &PluginServer{
		registry:           registry,
		socketPath:         socketPath,
		nativePlugins:      nativePlugins,
		pluginSyncRegister: make(chan plugin.PluginRegistration, 8),
		plugins:            make(map[string]pluginSdk, 0),
		Ready:              make(chan struct{}),
	}

	return pluginServer
}

func (s *PluginServer) Start(ctx context.Context) error {
	path := s.socketPath
	if after, ok := strings.CutPrefix(path, "unix://"); ok {
		path = after
	}

	// Remove existing socket if any
	if _, err := os.Stat(path); err == nil {
		os.Remove(path)
	}

	lis, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", path, err)
	}
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)

	logger.L().Info("plugin server listening: plugin server socket established and active", logger.Component("plugin-server"), logger.String("socket", s.socketPath))

	go func() {
		if err := srv.Serve(lis); err != nil {
			logger.L().Error("plugin server failed: grpc server exited with error", logger.Component("plugin-server"), logger.Error(err))
		}
	}()

	for _, plugin := range s.nativePlugins {
		s.registerPlugin(plugin)
	}

	close(s.Ready)
	<-ctx.Done()

	srv.GracefulStop()
	return nil
}

func (s *PluginServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch action.Type {
	case plugin.ActionRegisterPlugin:

		var pluginRegistration plugin.PluginRegistration
		if err := json.Unmarshal(action.Body, &pluginRegistration); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal registration: %v", err)
		}

		for cap := range pluginRegistration.Schemas {
			if strings.HasPrefix(cap, "__internal.") || strings.HasPrefix(cap, "std/") {
				logger.L().Error("plugin registration rejected: plugin attempted to register protected capability", logger.Component("plugin-server"), logger.Capability(cap), logger.Namespace(pluginRegistration.Namespace))
				return status.Errorf(codes.PermissionDenied, "plugin %s attempted to register protected capability %s", pluginRegistration.Namespace, cap)
			}
		}

		pluginRegistered := &pluginRemote{
			pluginRegistration: pluginRegistration,
		}
		s.registerPlugin(pluginRegistered)

		return stream.Send(&flight.Result{Body: []byte("OK")})

	case plugin.ActionPluginHeartbeat:
		var heartbeat plugin.Heartbeat
		if err := json.Unmarshal(action.Body, &heartbeat); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal heartbeat: %v", err)
		}
		logger.L().Debug("plugin heartbeat received: processing active plugin heartbeat", logger.Component("plugin-server"), logger.Namespace(heartbeat.Namespace))

		s.pluginsMU.RLock()
		defer s.pluginsMU.RUnlock()
		if info, ok := s.plugins[heartbeat.Namespace]; ok {
			info.LastHeartbeat(heartbeat)
		}

		return stream.Send(&flight.Result{Body: []byte("OK")})

	default:
		return status.Errorf(codes.Unimplemented, "action %s not implemented", action.Type)
	}
}

func (s *PluginServer) registerPlugin(body pluginSdk) {
	s.pluginsMU.Lock()
	defer s.pluginsMU.Unlock()

	namespace := body.PluginRegistration().Namespace
	s.plugins[namespace] = body

	logger.L().Info("plugin registered: successfully added new plugin capabilities to server registry", logger.Component("plugin-server"), logger.Namespace(namespace))
	s.pluginSyncRegister <- body.PluginRegistration()
}

func (s *PluginServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	// Handle bidirectional communication with plugin
	md, _ := metadata.FromIncomingContext(stream.Context())
	namespaces := md.Get("x-heddle-plugin-namespace")
	if len(namespaces) == 0 {
		return status.Error(codes.Unauthenticated, "missing plugin namespace")
	}
	namespace := namespaces[0]

	s.pluginsMU.RLock()
	pluginRegistered, ok := s.plugins[namespace]
	s.pluginsMU.RUnlock()

	if !ok {
		return status.Errorf(codes.NotFound, "plugin %s not registered", namespace)
	}
	pluginRegistered.Stream(stream)

	logger.L().Info("plugin stream connected: active plugin established exchange channel", logger.Component("plugin-server"), logger.Namespace(namespace))

	for {
		data, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				logger.L().Info("plugin stream disconnected: connection closed gracefully", logger.Component("plugin-server"), logger.Namespace(namespace))
				return nil
			}
			if status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
				logger.L().Info("plugin stream canceled: stream operation canceled by context", logger.Component("plugin-server"), logger.Namespace(namespace))
				return err
			}
			logger.L().Error("plugin stream error: connection closed unexpectedly", logger.Component("plugin-server"), logger.Namespace(namespace), logger.Error(err))
			return err
		}

		var resp plugin.ExecuteStepResponse
		if err := json.Unmarshal(data.DataBody, &resp); err != nil {
			logger.L().Error("plugin response invalid: failed to unmarshal json payload", logger.Component("plugin-server"), logger.Error(err))
			continue
		}

		// Layer 2: Validate OutputHandles path before registering
		for _, outPath := range resp.OutputRef {
			if outPath != "" {
				if err := validateSHMPath(outPath); err != nil {
					logger.L().Error("plugin security validation failed: shm path verification failed", logger.Component("plugin-server"), logger.String("path", outPath), logger.Error(err))
					resp.Status = models.TaskStatusFailed
					resp.ErrorMessage = "security error: invalid output handle path"
				}
			}
		}
	}
}

func (s *PluginServer) PluginSyncRegiter() <-chan plugin.PluginRegistration {
	return s.pluginSyncRegister
}

func (s *PluginServer) DispatchTask(ctx context.Context, task models.StepExecutionTask) (models.TaskResult, error) {
	namespace := task.Step.Call[0]

	s.pluginsMU.RLock()
	defer s.pluginsMU.RUnlock()
	pluginRegistered, ok := s.plugins[namespace]
	if !ok {
		logger.L().Warn("task dispatch failed: plugin not found in server registry",
			logger.Component("plugin-server"),
			logger.TraceID(task.WorkflowID),
			logger.TaskID(task.TaskID),
			logger.Namespace(namespace),
		)
		return models.TaskResult{}, fmt.Errorf("plugin %s not found", namespace)
	}

	if !pluginRegistered.HaveStream() {
		logger.L().Warn("task dispatch failed: plugin found but not connected to stream",
			logger.Component("plugin-server"),
			logger.TraceID(task.WorkflowID),
			logger.TaskID(task.TaskID),
			logger.Namespace(namespace),
		)
		return models.TaskResult{}, fmt.Errorf("plugin %s not connected", namespace)
	}

	// Zero-Copy Input: resolve SHM path from registry metadata
	var inputHandles map[string]string

	if s.registry != nil {
		inputHandles = make(map[string]string)
		
		prevIDs := task.PreviousTaskIDs
		if len(prevIDs) == 0 && task.PreviousTaskID != "" {
			prevIDs = []string{task.PreviousTaskID}
		}

		for _, handle := range prevIDs {
			if handle == "" {
				continue
			}
			meta, ok := s.registry.GetMetadata(task.WorkflowID, handle, locality.Output)
			if ok {
				// Layer 2: Validate SHM path before dispatching
				for _, p := range meta.Paths {
					if err := validateSHMPath(p); err != nil {
						logger.L().Error("task dispatch failed: registry contains invalid SHM path",
							logger.Component("plugin-server"),
							logger.TraceID(task.WorkflowID),
							logger.TaskID(task.TaskID),
							logger.Error(err),
						)
						return models.TaskResult{}, fmt.Errorf("security error: registry contains invalid SHM path: %w", err)
					}
				}
				
				// Apply column prefixing using parent assignment names if available
				assignmentName := task.ParentAssignments[handle]
				for colName, path := range meta.Paths {
					key := colName
					if assignmentName != "" {
						key = fmt.Sprintf("%s_%s", assignmentName, colName)
					}
					inputHandles[key] = path
				}
			} else {
				logger.L().Error("task dispatch failed: input data for task not found in registry",
					logger.Component("plugin-server"),
					logger.TraceID(task.WorkflowID),
					logger.TaskID(task.TaskID),
					logger.String("missing_handle", handle),
				)
				return models.TaskResult{}, fmt.Errorf("critical error: input data for task %s (from previous task %s) not found in registry", task.TaskID, handle)
			}
		}
	}

	configJSON, _ := json.Marshal(task.Step.Config)
	var resourceId string
	for _, resName := range task.Step.Resources {
		resourceId = resName
		break
	}

	request := plugin.ExecuteStepRequest{
		WorkflowID:  task.WorkflowID,
		TaskID:      task.TaskID,
		StepName:    task.Step.Call[1],
		ResourceRef: resourceId,
		ConfigJSON:  string(configJSON),
		InputRef:    inputHandles,
		Resources:   task.Resources,
	}

	logger.L().Debug("task dispatch initiated: transmitting request payload to registered plugin",
		logger.Component("plugin-server"),
		logger.TraceID(task.WorkflowID),
		logger.TaskID(task.TaskID),
		logger.Namespace(namespace),
	)

	if err := pluginRegistered.Send(ctx, request); err != nil {
		logger.L().Error("task dispatch failed: sending request to plugin failed",
			logger.Component("plugin-server"),
			logger.TraceID(task.WorkflowID),
			logger.TaskID(task.TaskID),
			logger.Error(err),
		)
		return models.TaskResult{}, fmt.Errorf("failed to send task to plugin: %w", err)
	}

	resp, err := pluginRegistered.Recv()
	if err != nil {
		logger.L().Error("task dispatch failed: receiving result from plugin failed",
			logger.Component("plugin-server"),
			logger.TraceID(task.WorkflowID),
			logger.TaskID(task.TaskID),
			logger.Error(err),
		)
		return models.TaskResult{}, fmt.Errorf("failed to receive result from plugin: %w", err)
	}

	// Zero-Copy Output: register SHM path in registry
	if s.registry != nil {
		// Layer 2: validateSHMPath was already done in DoExchange, but we do it again for defense in depth
		for _, outPath := range resp.OutputRef {
			if err := validateSHMPath(outPath); err != nil {
				logger.L().Error("task dispatch failed: plugin returned invalid SHM path",
					logger.Component("plugin-server"),
					logger.TraceID(task.WorkflowID),
					logger.TaskID(task.TaskID),
					logger.Error(err),
				)
				return models.TaskResult{}, fmt.Errorf("security error: plugin returned invalid SHM path: %w", err)
			}
		}

		handle := task.TaskID
		// Layer 4: Registry.Put now validates permissions/ownership
		if err := s.registry.Put(locality.NewMetadata(task.WorkflowID, handle, locality.Output, resp.OutputRef)); err != nil {
			logger.L().Error("task dispatch failed: failed to register SHM output metadata",
				logger.Component("plugin-server"),
				logger.TraceID(task.WorkflowID),
				logger.TaskID(task.TaskID),
				logger.Error(err),
			)
			return models.TaskResult{}, fmt.Errorf("failed to register SHM output: %w", err)
		}
		logger.L().Info("registry update completed: registered shm output metadata", logger.Component("plugin-server"), logger.TraceID(task.WorkflowID), logger.TaskID(handle))
	}

	return models.TaskResult{
		TaskID:        resp.TaskID,
		Status:        string(resp.Status),
		ErrorMessage:  resp.ErrorMessage,
		OutputHandles: resp.OutputRef,
	}, nil
}

// validateSHMPath ensures the path is inside /dev/shm and doesn't contain traversal.
func validateSHMPath(path string) error {
	if !strings.HasPrefix(path, "/dev/shm/") {
		return fmt.Errorf("path %s is not in /dev/shm", path)
	}
	if strings.Contains(path, "..") {
		return fmt.Errorf("path %s contains traversal characters", path)
	}
	return nil
}
