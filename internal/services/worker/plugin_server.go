package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"

	"sync"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	logger.L().Info("Plugin connected to exchange stream")

	for {
		data, err := stream.Recv()
		if err != nil {
			logger.L().Error("Plugin stream closed", zap.Error(err))
			return err
		}

		// For now, just echo or log
		_ = data
	}
}

func NewPluginServer(socketPath string) *PluginServer {
	return &PluginServer{
		SocketPath: socketPath,
	}
}
