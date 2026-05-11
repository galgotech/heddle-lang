package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	"sync"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

type PluginServer struct {
	flight.BaseFlightServer
	SocketPath           string
	Plugins              sync.Map // map[string]*PluginInfo
	OnCapabilitiesUpdate func(ctx context.Context, capabilities []string) error
	Ready                chan struct{}
	Registry             *locality.DataLocalityRegistry
}

type PluginInfo struct {
	Registration plugin.PluginRegistration
	Namespace    string
	Stream       flight.FlightService_DoExchangeServer
	ResponseCh   map[string]chan plugin.ExecuteStepResponse
	mu           sync.Mutex
}

func (s *PluginServer) Start(ctx context.Context) error {
	// Remove existing socket if any
	if _, err := os.Stat(s.SocketPath); err == nil {
		os.Remove(s.SocketPath)
	}

	lis, err := net.Listen("unix", s.SocketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.SocketPath, err)
	}
	defer lis.Close()

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)

	logger.L().Info("Plugin server listening", zap.String("socket", s.SocketPath))

	go func() {
		if err := srv.Serve(lis); err != nil {
			logger.L().Error("Plugin server failed", zap.Error(err))
		}
	}()

	if s.Ready != nil {
		close(s.Ready)
	}

	<-ctx.Done()
	srv.GracefulStop()
	return nil
}

func (s *PluginServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch action.Type {
	case plugin.ActionRegisterPlugin:
		var reg plugin.PluginRegistration
		if err := json.Unmarshal(action.Body, &reg); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal registration: %v", err)
		}
		s.Plugins.Store(reg.Namespace, &PluginInfo{
			Registration: reg,
			Namespace:    reg.Namespace,
		})
		logger.L().Info("Plugin registered", zap.String("namespace", reg.Namespace), zap.String("language", reg.Language))

		if s.OnCapabilitiesUpdate != nil {
			if err := s.OnCapabilitiesUpdate(stream.Context(), reg.Capabilities); err != nil {
				logger.L().Error("Failed to update capabilities", zap.Error(err))
			}
		}

		return stream.Send(&flight.Result{Body: []byte("OK")})

	case plugin.ActionPluginHeartbeat:
		var hb plugin.Heartbeat
		if err := json.Unmarshal(action.Body, &hb); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to unmarshal heartbeat: %v", err)
		}
		logger.L().Info("Heartbeat from plugin")
		return stream.Send(&flight.Result{Body: []byte("OK")})

	default:
		return status.Errorf(codes.Unimplemented, "action %s not implemented", action.Type)
	}
}

func (s *PluginServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	// Handle bidirectional communication with plugin
	md, _ := metadata.FromIncomingContext(stream.Context())
	namespaces := md.Get("x-heddle-plugin-namespace")
	if len(namespaces) == 0 {
		return status.Error(codes.Unauthenticated, "missing plugin namespace")
	}
	namespace := namespaces[0]

	val, ok := s.Plugins.Load(namespace)
	if !ok {
		return status.Errorf(codes.NotFound, "plugin %s not registered", namespace)
	}

	info := val.(*PluginInfo)
	info.Stream = stream
	info.ResponseCh = make(map[string]chan plugin.ExecuteStepResponse)

	logger.L().Info("Plugin connected to exchange stream", zap.String("namespace", namespace))

	for {
		data, err := stream.Recv()
		if err != nil {
			logger.L().Error("Plugin stream closed", zap.Error(err), zap.String("namespace", namespace))
			return err
		}

		var resp plugin.ExecuteStepResponse
		if err := json.Unmarshal(data.DataBody, &resp); err != nil {
			logger.L().Error("Failed to unmarshal plugin response", zap.Error(err))
			continue
		}

		// Layer 2: Validate OutputHandle path before registering
		if resp.OutputHandle != "" {
			if err := validateSHMPath(resp.OutputHandle); err != nil {
				logger.L().Error("Plugin provided invalid SHM output path", zap.Error(err), zap.String("path", resp.OutputHandle))
				resp.Status = models.TaskStatusFailed
				resp.ErrorMessage = "security error: invalid output handle path"
			}
		}

		info.mu.Lock()
		if ch, ok := info.ResponseCh[resp.TaskID]; ok {
			ch <- resp
		}
		info.mu.Unlock()
	}
}

func (s *PluginServer) DispatchTask(ctx context.Context, task models.StepExecutionTask) (models.TaskResult, error) {
	namespace := task.Step.Call[0]
	val, ok := s.Plugins.Load(namespace)
	if !ok {
		return models.TaskResult{}, fmt.Errorf("plugin %s not found", namespace)
	}
	info := val.(*PluginInfo)

	if info.Stream == nil {
		return models.TaskResult{}, fmt.Errorf("plugin %s not connected", namespace)
	}

	// Zero-Copy Input: resolve SHM path from registry metadata
	inputHandle := ""
	isInputVoid := len(task.Step.InputType) > 0 && task.Step.InputType[0] == models.VoidType
	if s.Registry != nil && !isInputVoid {
		handle := task.PreviousTaskID
		if handle != "" {
			meta, ok := s.Registry.GetMetadata(task.WorkflowID, handle, locality.Output)
			if ok {
				// Layer 2: Validate SHM path before dispatching
				if err := validateSHMPath(meta.Path); err != nil {
					return models.TaskResult{}, fmt.Errorf("security error: registry contains invalid SHM path: %w", err)
				}
				inputHandle = meta.Path
			} else {
				return models.TaskResult{}, fmt.Errorf("critical error: input data for task %s (from previous task %s) not found in registry", task.TaskID, handle)
			}
		} else {
			return models.TaskResult{}, fmt.Errorf("critical error: task %s expects input but PreviousTaskID is empty", task.TaskID)
		}
	}

	configJSON, _ := json.Marshal(task.Step.Config)
	req := plugin.ExecuteStepRequest{
		TaskID:      task.TaskID,
		StepName:    task.Step.Call[1],
		ConfigJSON:  string(configJSON),
		InputHandle: inputHandle,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return models.TaskResult{}, fmt.Errorf("failed to marshal task to plugin: %w", err)
	}

	resCh := make(chan plugin.ExecuteStepResponse, 1)
	info.mu.Lock()
	info.ResponseCh[task.TaskID] = resCh
	info.mu.Unlock()
	defer func() {
		info.mu.Lock()
		delete(info.ResponseCh, task.TaskID)
		info.mu.Unlock()
	}()

	if err := info.Stream.Send(&flight.FlightData{DataBody: body}); err != nil {
		return models.TaskResult{}, fmt.Errorf("failed to send task to plugin: %w", err)
	}

	select {
	case <-ctx.Done():
		return models.TaskResult{}, ctx.Err()
	case resp := <-resCh:
		// Zero-Copy Output: register SHM path in registry
		isOutputVoid := len(task.Step.OutputType) > 0 && task.Step.OutputType[0] == models.VoidType
		if resp.OutputHandle != "" && s.Registry != nil && !isOutputVoid {
			// Layer 2: validateSHMPath was already done in DoExchange, but we do it again for defense in depth
			if err := validateSHMPath(resp.OutputHandle); err != nil {
				return models.TaskResult{}, fmt.Errorf("security error: plugin returned invalid SHM path: %w", err)
			}

			handle := task.TaskID
			// Layer 4: Registry.Put now validates permissions/ownership
			if err := s.Registry.Put(locality.NewMetadata(task.WorkflowID, handle, locality.Output, resp.OutputHandle)); err != nil {
				return models.TaskResult{}, fmt.Errorf("failed to register SHM output: %w", err)
			}
			logger.L().Info("Registered SHM output in registry", zap.String("handle", handle), zap.String("path", resp.OutputHandle))
		}

		return models.TaskResult{
			TaskID:       resp.TaskID,
			Status:       resp.Status,
			ErrorMessage: resp.ErrorMessage,
		}, nil
	}
}

func NewPluginServer(socketPath string) *PluginServer {
	return &PluginServer{
		SocketPath: socketPath,
		Ready:      make(chan struct{}),
	}
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
