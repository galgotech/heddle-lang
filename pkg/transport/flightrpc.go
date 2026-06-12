package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// FlightClient is a socket/gRPC-based implementation of Client using Arrow Flight.
type FlightClient struct {
	client flight.Client
}

// NewFlightClient creates a new FlightClient wrapping a flight.Client.
func NewFlightClient(client flight.Client) *FlightClient {
	logger.L().Debug("client: initializing flight client", logger.Component("transport"))
	return &FlightClient{client: client}
}

// DoAction executes an action via the remote Flight service.
func (c *FlightClient) DoAction(ctx context.Context, action *Action) (ResultStream, error) {
	logger.L().Debug("client: executing action via flight service", logger.Component("transport"), logger.String("action_type", action.Type))
	fAction := &flight.Action{
		Type: action.Type,
		Body: action.Body,
	}
	resStream, err := c.client.DoAction(ctx, fAction)
	if err != nil {
		logger.L().Error("client: failed to execute action via flight service", logger.Component("transport"), logger.String("action_type", action.Type), logger.Error(err))
		return nil, err
	}
	return &flightResultStream{stream: resStream}, nil
}

// DoExchange opens a bidirectional exchange stream via the remote Flight service.
func (c *FlightClient) DoExchange(ctx context.Context) (ExchangeStream, error) {
	logger.L().Debug("client: opening bidirectional exchange stream via flight service", logger.Component("transport"))
	exStream, err := c.client.DoExchange(ctx)
	if err != nil {
		logger.L().Error("client: failed to open bidirectional exchange stream via flight service", logger.Component("transport"), logger.Error(err))
		return nil, err
	}
	return &flightExchangeStream{stream: exStream}, nil
}

type flightResultStream struct {
	stream flight.FlightService_DoActionClient
}

func (s *flightResultStream) Recv() (*Result, error) {
	logger.L().Debug("stream: receiving result from flight stream", logger.Component("transport"))
	res, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			logger.L().Debug("stream: result stream closed by peer", logger.Component("transport"))
		} else {
			logger.L().Error("stream: failed to receive result from flight stream", logger.Component("transport"), logger.Error(err))
		}
		return nil, err
	}
	return &Result{Body: res.Body}, nil
}

type flightExchangeStream struct {
	stream flight.FlightService_DoExchangeClient
}

func (s *flightExchangeStream) Send(data *FlightData) error {
	logger.L().Debug("stream: sending flight data to exchange stream", logger.Component("transport"))
	err := s.stream.Send(&flight.FlightData{
		AppMetadata: data.AppMetadata,
		DataBody:    data.DataBody,
	})
	if err != nil {
		if isCanceledError(err) {
			logger.L().Debug("stream: failed to send flight data to exchange stream due to cancellation", logger.Component("transport"), logger.Error(err))
		} else {
			logger.L().Error("stream: failed to send flight data to exchange stream", logger.Component("transport"), logger.Error(err))
		}
		return err
	}
	return nil
}

func (s *flightExchangeStream) Recv() (*FlightData, error) {
	logger.L().Debug("stream: receiving flight data from exchange stream", logger.Component("transport"))
	fd, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			logger.L().Debug("stream: exchange stream closed by peer", logger.Component("transport"))
		} else if isCanceledError(err) {
			logger.L().Debug("stream: exchange stream canceled", logger.Component("transport"), logger.Error(err))
		} else {
			logger.L().Error("stream: failed to receive flight data from exchange stream", logger.Component("transport"), logger.Error(err))
		}
		return nil, err
	}
	return &FlightData{
		AppMetadata: fd.AppMetadata,
		DataBody:    fd.DataBody,
	}, nil
}

// Connect connects to a remote Control Plane over gRPC/Arrow Flight.
func Connect(addr string) (Client, error) {
	if (strings.HasPrefix(addr, "/") || strings.HasPrefix(addr, "./") || strings.HasSuffix(addr, ".sock")) && !strings.Contains(addr, "://") {
		addr = "unix://" + addr
	}
	logger.L().Debug("client: connecting to remote control plane", logger.Component("transport"), logger.String("address", addr))
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.L().Error("client: failed to connect to control plane", logger.Component("transport"), logger.String("address", addr), logger.Error(err))
		return nil, fmt.Errorf("failed to connect to control plane: %w", err)
	}
	logger.L().Info("client: successfully connected to control plane", logger.Component("transport"), logger.String("address", addr))
	flightClient := flight.NewClientFromConn(conn, nil)
	return NewFlightClient(flightClient), nil
}

// FlightServer implements flight.FlightServiceServer wrapping our transport.Server interface.
type FlightServer struct {
	flight.BaseFlightServer
	handler Server
}

func NewFlightServer(handler Server) *FlightServer {
	logger.L().Debug("server: initializing flight server", logger.Component("transport"))
	return &FlightServer{handler: handler}
}

type flightActionServerStream struct {
	stream flight.FlightService_DoActionServer
}

func (s *flightActionServerStream) Send(res *Result) error {
	logger.L().Debug("stream: sending result to action server stream", logger.Component("transport"))
	err := s.stream.Send(&flight.Result{Body: res.Body})
	if err != nil {
		logger.L().Error("stream: failed to send result to action server stream", logger.Component("transport"), logger.Error(err))
		return err
	}
	return nil
}

func (fs *FlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	logger.L().Debug("server: received action via flight service", logger.Component("transport"), logger.String("action_type", action.Type))
	tAction := &Action{
		Type: action.Type,
		Body: action.Body,
	}
	tStream := &flightActionServerStream{stream: stream}
	err := fs.handler.DoAction(stream.Context(), tAction, tStream)
	if err != nil {
		logger.L().Error("server: failed to process action via flight service", logger.Component("transport"), logger.String("action_type", action.Type), logger.Error(err))
	}
	return err
}

type flightExchangeServerStream struct {
	stream flight.FlightService_DoExchangeServer
}

func (s *flightExchangeServerStream) Send(data *FlightData) error {
	logger.L().Debug("stream: sending flight data to exchange server stream", logger.Component("transport"))
	err := s.stream.Send(&flight.FlightData{
		AppMetadata: data.AppMetadata,
		DataBody:    data.DataBody,
	})
	if err != nil {
		if isCanceledError(err) {
			logger.L().Debug("stream: failed to send flight data to exchange server stream due to cancellation", logger.Component("transport"), logger.Error(err))
		} else {
			logger.L().Error("stream: failed to send flight data to exchange server stream", logger.Component("transport"), logger.Error(err))
		}
		return err
	}
	return nil
}

func (s *flightExchangeServerStream) Recv() (*FlightData, error) {
	logger.L().Debug("stream: receiving flight data from exchange server stream", logger.Component("transport"))
	fd, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			logger.L().Debug("stream: exchange server stream closed by peer", logger.Component("transport"))
		} else if isCanceledError(err) {
			logger.L().Debug("stream: exchange server stream canceled", logger.Component("transport"), logger.Error(err))
		} else {
			logger.L().Error("stream: failed to receive flight data from exchange server stream", logger.Component("transport"), logger.Error(err))
		}
		return nil, err
	}
	return &FlightData{
		AppMetadata: fd.AppMetadata,
		DataBody:    fd.DataBody,
	}, nil
}

func (fs *FlightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	logger.L().Debug("server: received exchange stream request via flight service", logger.Component("transport"))
	tStream := &flightExchangeServerStream{stream: stream}
	err := fs.handler.DoExchange(stream.Context(), tStream)
	if err != nil {
		if isCanceledError(err) {
			logger.L().Debug("server: exchange stream canceled", logger.Component("transport"), logger.Error(err))
		} else {
			logger.L().Error("server: failed to process exchange stream via flight service", logger.Component("transport"), logger.Error(err))
		}
	}
	return err
}

// StartFlightServer starts the gRPC and Flight service listeners on the target address (handling TCP or Unix domain sockets).
func StartFlightServer(addr string, srv Server, onReady func()) error {
	var lis net.Listener
	var err error

	if after, ok := strings.CutPrefix(addr, "unix://"); ok {
		path := after
		if _, err := os.Stat(path); err == nil {
			logger.L().Debug("socket: removing existing socket file", logger.Component("transport"), logger.String("path", path))
			os.Remove(path)
		}
		lis, err = net.Listen("unix", path)
	} else {
		lis, err = net.Listen("tcp", addr)
	}

	if err != nil {
		logger.L().Error("listener: failed to listen on address", logger.Component("transport"), logger.String("address", addr), logger.Error(err))
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	logger.L().Debug("grpc: registering flight service server", logger.Component("transport"))
	grpcServer := grpc.NewServer()
	flightServer := NewFlightServer(srv)
	flight.RegisterFlightServiceServer(grpcServer, flightServer)

	errCh := make(chan error, 1)
	go func() {
		logger.L().Info("server: starting flight server listener", logger.Component("transport"), logger.String("address", addr))
		errCh <- grpcServer.Serve(lis)
	}()

	if onReady != nil {
		onReady()
	}

	return <-errCh
}

// NewExchangeServerStream wraps a raw flight.FlightService_DoExchangeServer into a transport.ExchangeStream.
func NewExchangeServerStream(stream flight.FlightService_DoExchangeServer) ExchangeStream {
	return &flightExchangeServerStream{stream: stream}
}

// NewActionServerStream wraps a raw flight.FlightService_DoActionServer into a transport.ServerStream.
func NewActionServerStream(stream flight.FlightService_DoActionServer) ServerStream {
	return &flightActionServerStream{stream: stream}
}

// FlightServerTransport is a gRPC/Arrow Flight implementation of ServerTransport.
type FlightServerTransport struct {
	addr  string
	srv   Server
	Ready chan struct{}
}

func NewFlightServerTransport(addr string) *FlightServerTransport {
	return &FlightServerTransport{
		addr:  addr,
		Ready: make(chan struct{}),
	}
}

func (t *FlightServerTransport) SetServer(srv Server) {
	t.srv = srv
}

func (t *FlightServerTransport) Start() error {
	logger.L().Info("transport: starting flight server transport", logger.Component("transport"), logger.String("address", t.addr))
	onReady := func() {
		close(t.Ready)
	}
	return StartFlightServer(t.addr, t.srv, onReady)
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
	return strings.Contains(err.Error(), "context canceled")
}
