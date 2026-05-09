package controlplane

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
)

// FlightServer implements the Arrow Flight interface for the Heddle Control Plane.
type FlightServer struct {
	flight.BaseFlightServer
	controlPlane *ControlPlane
}

func (s *FlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	workerID := GetWorkerID(stream.Context())

	switch action.Type {
	case execution.ActionRegisterWorker:
		var reg execution.WorkerRegistration
		if err := json.Unmarshal(action.Body, &reg); err != nil {
			return fmt.Errorf("failed to unmarshal registration: %w", err)
		}
		s.controlPlane.RegisterWorker(reg, workerID)
		return stream.Send(&flight.Result{Body: []byte("OK")})

	case execution.ActionHeartbeat:
		var hb execution.Heartbeat
		if err := json.Unmarshal(action.Body, &hb); err != nil {
			return fmt.Errorf("failed to unmarshal heartbeat: %w", err)
		}
		if err := s.controlPlane.Heartbeat(hb, workerID); err != nil {
			return err
		}
		return stream.Send(&flight.Result{Body: []byte("OK")})

	case execution.ActionSubmitWorkflow:
		source := string(action.Body)
		if err := s.controlPlane.SubmitWorkflow(source); err != nil {
			return err
		}
		return stream.Send(&flight.Result{Body: []byte("Workflow initialized successfully")})

	default:
		return fmt.Errorf("unknown action: %s", action.Type)
	}
}

func (s *FlightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	return s.controlPlane.Exchange(stream)
}

// NewFlightServer creates a new FlightServer wrapping a ControlPlane.
func NewFlightServer(cp *ControlPlane) *FlightServer {
	return &FlightServer{controlPlane: cp}
}

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

	workerRegistry := NewWorkerRegistry()
	workQueue := NewWorkQueue()
	dataLocality := NewDataLocalityRegistry()

	controlPlane := NewControlPlane(workerRegistry, workQueue, dataLocality)
	controlPlane.Start()

	flightServer := NewFlightServer(controlPlane)
	flight.RegisterFlightServiceServer(server, flightServer)

	logger.L().Info("Control Plane Flight Server listening", zap.String("address", lis.Addr().String()))
	if err := server.Serve(lis); err != nil {
		logger.L().Fatal("failed to serve", zap.Error(err))
	}
}
