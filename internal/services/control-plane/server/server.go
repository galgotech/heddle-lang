package server

import (
	"fmt"
	"net"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	"github.com/galgotech/heddle-lang/internal/services/control-plane/manager"
	"github.com/galgotech/heddle-lang/internal/services/control-plane/scheduler"
	"github.com/galgotech/heddle-lang/internal/services/control-plane/state"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

func ListenAndServe(port int) {
	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.L().Fatal("failed to listen", zap.Error(err))
	}
	Serve(lis)
}

func Serve(lis net.Listener) {
	server := grpc.NewServer(
		grpc.UnaryInterceptor(UnaryWorkerInterceptor),
		grpc.StreamInterceptor(StreamWorkerInterceptor),
	)

	registry := manager.NewRegistry()
	queue := scheduler.NewWorkQueue(rate.Limit(100), 10, nil)
	sm := state.NewStateMachine()
	locality := manager.NewDataLocalityRegistry()

	cpServer := NewControlPlaneServer(registry, queue, sm, locality)
	flight.RegisterFlightServiceServer(server, cpServer)

	logger.L().Info("Control Plane Flight Server listening", zap.String("address", lis.Addr().String()))
	if err := server.Serve(lis); err != nil {
		logger.L().Fatal("failed to serve", zap.Error(err))
	}
}
