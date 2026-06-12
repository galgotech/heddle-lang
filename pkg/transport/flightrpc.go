package transport

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// FlightClient is a socket/gRPC-based implementation of Client using Arrow Flight.
type FlightClient struct {
	client flight.Client
}

// NewFlightClient creates a new FlightClient wrapping a flight.Client.
func NewFlightClient(client flight.Client) *FlightClient {
	return &FlightClient{client: client}
}

// DoAction executes an action via the remote Flight service.
func (c *FlightClient) DoAction(ctx context.Context, action *Action) (ResultStream, error) {
	fAction := &flight.Action{
		Type: action.Type,
		Body: action.Body,
	}
	resStream, err := c.client.DoAction(ctx, fAction)
	if err != nil {
		return nil, err
	}
	return &flightResultStream{stream: resStream}, nil
}

// DoExchange opens a bidirectional exchange stream via the remote Flight service.
func (c *FlightClient) DoExchange(ctx context.Context) (ExchangeStream, error) {
	exStream, err := c.client.DoExchange(ctx)
	if err != nil {
		return nil, err
	}
	return &flightExchangeStream{stream: exStream}, nil
}

type flightResultStream struct {
	stream flight.FlightService_DoActionClient
}

func (s *flightResultStream) Recv() (*Result, error) {
	res, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	return &Result{Body: res.Body}, nil
}

type flightExchangeStream struct {
	stream flight.FlightService_DoExchangeClient
}

func (s *flightExchangeStream) Send(data *FlightData) error {
	return s.stream.Send(&flight.FlightData{
		AppMetadata: data.AppMetadata,
		DataBody:    data.DataBody,
	})
}

func (s *flightExchangeStream) Recv() (*FlightData, error) {
	fd, err := s.stream.Recv()
	if err != nil {
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
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to control plane: %w", err)
	}
	flightClient := flight.NewClientFromConn(conn, nil)
	return NewFlightClient(flightClient), nil
}

// FlightServer implements flight.FlightServiceServer wrapping our transport.Server interface.
type FlightServer struct {
	flight.BaseFlightServer
	handler Server
}

func NewFlightServer(handler Server) *FlightServer {
	return &FlightServer{handler: handler}
}

type flightActionServerStream struct {
	stream flight.FlightService_DoActionServer
}

func (s *flightActionServerStream) Send(res *Result) error {
	return s.stream.Send(&flight.Result{Body: res.Body})
}

func (fs *FlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	tAction := &Action{
		Type: action.Type,
		Body: action.Body,
	}
	tStream := &flightActionServerStream{stream: stream}
	return fs.handler.DoAction(stream.Context(), tAction, tStream)
}

type flightExchangeServerStream struct {
	stream flight.FlightService_DoExchangeServer
}

func (s *flightExchangeServerStream) Send(data *FlightData) error {
	return s.stream.Send(&flight.FlightData{
		AppMetadata: data.AppMetadata,
		DataBody:    data.DataBody,
	})
}

func (s *flightExchangeServerStream) Recv() (*FlightData, error) {
	fd, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	return &FlightData{
		AppMetadata: fd.AppMetadata,
		DataBody:    fd.DataBody,
	}, nil
}

func (fs *FlightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	tStream := &flightExchangeServerStream{stream: stream}
	return fs.handler.DoExchange(stream.Context(), tStream)
}

// StartFlightServer starts the gRPC and Flight service listeners on the target address (handling TCP or Unix domain sockets).
func StartFlightServer(addr string, srv Server, onReady func()) error {
	var lis net.Listener
	var err error

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

	grpcServer := grpc.NewServer()
	flightServer := NewFlightServer(srv)
	flight.RegisterFlightServiceServer(grpcServer, flightServer)

	errCh := make(chan error, 1)
	go func() {
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
	onReady := func() {
		close(t.Ready)
	}
	return StartFlightServer(t.addr, t.srv, onReady)
}
