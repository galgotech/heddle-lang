package transport

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/pkg/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// InMemory is an in-memory gRPC-based implementation of Client using ControlPlaneUDSPath.
type InMemory struct {
	mu         sync.RWMutex
	server     Server
	lis        net.Listener
	grpcServer *grpc.Server
	client     *FlightClient
	conn       *grpc.ClientConn
	socketPath string
}

// NewInMemory creates a in-memory Client and Server.
func NewInMemory(server Server) *InMemory {
	addr := runtime.ControlPlaneUDSPath

	network := "tcp"
	address := addr
	var socketPath string

	if after, ok := strings.CutPrefix(addr, "unix://"); ok {
		network = "unix"
		address = after
		socketPath = after
		// Remove existing socket file if it exists
		if _, err := os.Stat(address); err == nil {
			_ = os.Remove(address)
		}
	}

	lis, err := net.Listen(network, address)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on %s: %v", addr, err))
	}

	inMemory := &InMemory{
		server:     server,
		lis:        lis,
		socketPath: socketPath,
	}

	// Setup gRPC server
	grpcServer := grpc.NewServer()
	proxy := &serverProxy{inMemory: inMemory}
	flightServer := NewFlightServer(proxy)
	flight.RegisterFlightServiceServer(grpcServer, flightServer)
	inMemory.grpcServer = grpcServer

	// Start gRPC server in a goroutine
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	// Setup client connection using standard gRPC Dial to ControlPlaneUDSPath
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("failed to dial control plane socket: %v", err))
	}

	flightClient := flight.NewClientFromConn(conn, nil)
	inMemory.client = NewFlightClient(flightClient)
	inMemory.conn = conn

	return inMemory
}

// SetServer registers the server implementation.
func (c *InMemory) SetServer(server Server) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.server = server
}

func (c *InMemory) getServer() Server {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.server
}

// Start prepares the in-memory transport and signals that it is ready.
func (c *InMemory) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.server == nil {
		c.mu.Unlock()
		return fmt.Errorf("in-memory server not registered")
	}
	c.mu.Unlock()

	<-ctx.Done()
	c.Close()
	return nil
}

// Close closes the client connection, gRPC server, and the listener.
func (c *InMemory) Close() {
	c.mu.Lock()
	conn := c.conn
	grpcServer := c.grpcServer
	lis := c.lis
	socketPath := c.socketPath

	c.conn = nil
	c.grpcServer = nil
	c.lis = nil
	c.socketPath = ""
	c.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	if grpcServer != nil {
		grpcServer.GracefulStop()
	}
	if lis != nil {
		_ = lis.Close()
	}
	if socketPath != "" {
		_ = os.Remove(socketPath)
	}
}

// DoAction processes the action via the underlying FlightClient.
func (c *InMemory) DoAction(ctx context.Context, action *Action) (ResultStream, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	if client == nil {
		return nil, fmt.Errorf("in-memory client is closed")
	}
	return client.DoAction(ctx, action)
}

// inMemoryExchangeStream wraps flightExchangeStream to provide a Close method.
type inMemoryExchangeStream struct {
	*flightExchangeStream
}

// Close closes the send side of the stream.
func (s *inMemoryExchangeStream) Close() error {
	return s.stream.CloseSend()
}

// DoExchange opens an in-memory exchange stream pairing a client stream and a server stream.
func (c *InMemory) DoExchange(ctx context.Context) (ExchangeStream, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	if client == nil {
		return nil, fmt.Errorf("in-memory client is closed")
	}

	stream, err := client.DoExchange(ctx)
	if err != nil {
		return nil, err
	}

	fStream, ok := stream.(*flightExchangeStream)
	if !ok {
		return stream, nil
	}

	return &inMemoryExchangeStream{flightExchangeStream: fStream}, nil
}

// serverProxy is a proxy to forward calls dynamically to the server registered on InMemory.
type serverProxy struct {
	inMemory *InMemory
}

func (p *serverProxy) DoAction(ctx context.Context, action *Action, stream ServerStream) error {
	srv := p.inMemory.getServer()
	if srv == nil {
		return fmt.Errorf("in-memory server not registered")
	}
	return srv.DoAction(ctx, action, stream)
}

func (p *serverProxy) DoExchange(ctx context.Context, stream ExchangeStream) error {
	srv := p.inMemory.getServer()
	if srv == nil {
		return fmt.Errorf("in-memory server not registered")
	}
	return srv.DoExchange(ctx, stream)
}
