package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"sync"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type PluginServer struct {
	flight.BaseFlightServer
	SocketPath string
	Plugins    sync.Map // map[string]*PluginInfo
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

	var info *PluginInfo
	for i := 0; i < 5; i++ {
		val, ok := s.Plugins.Load(namespace)
		if ok {
			info = val.(*PluginInfo)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if info == nil {
		return status.Errorf(codes.NotFound, "plugin %s not registered", namespace)
	}
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

	configJSON, _ := json.Marshal(task.Step.Config)
	req := plugin.ExecuteStepRequest{
		TaskID:     task.TaskID,
		StepName:   task.Step.Call[1],
		ConfigJSON: string(configJSON),
	}
	body, _ := json.Marshal(req)

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
	}
}
