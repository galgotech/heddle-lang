package transport

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/pkg/logger"
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
			logger.L().Debug("socket: removing existing socket file", logger.Component("transport"), logger.String("path", address))
			_ = os.Remove(address)
		}
	}

	logger.L().Debug("listener: listening on address", logger.Component("transport"), logger.String("network", network), logger.String("address", address))
	lis, err := net.Listen(network, address)
	if err != nil {
		logger.L().Error("listener: failed to listen on address", logger.Component("transport"), logger.String("network", network), logger.String("address", address), logger.Error(err))
		panic(fmt.Sprintf("failed to listen on %s: %v", addr, err))
	}

	inMemory := &InMemory{
		server:     server,
		lis:        lis,
		socketPath: socketPath,
	}

	// Setup gRPC server
	logger.L().Debug("grpc: registering flight service server", logger.Component("transport"))
	grpcServer := grpc.NewServer()
	proxy := &serverProxy{inMemory: inMemory}
	flightServer := NewFlightServer(proxy)
	flight.RegisterFlightServiceServer(grpcServer, flightServer)
	inMemory.grpcServer = grpcServer

	// Start gRPC server in a goroutine
	go func() {
		logger.L().Debug("grpc: starting flight server listener", logger.Component("transport"))
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			logger.L().Error("grpc: flight server serve failed", logger.Component("transport"), logger.Error(err))
		}
	}()

	// Setup client connection using standard gRPC Dial to ControlPlaneUDSPath
	logger.L().Debug("client: dialing control plane socket", logger.Component("transport"), logger.String("address", addr))
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.L().Error("client: failed to dial control plane socket", logger.Component("transport"), logger.String("address", addr), logger.Error(err))
		panic(fmt.Sprintf("failed to dial control plane socket: %v", err))
	}
	logger.L().Debug("client: successfully connected to control plane", logger.Component("transport"), logger.String("address", addr))

	flightClient := flight.NewClientFromConn(conn, nil)
	inMemory.client = NewFlightClient(flightClient)
	inMemory.conn = conn

	return inMemory
}

// SetServer registers the server implementation.
func (c *InMemory) SetServer(server Server) {
	c.mu.Lock()
	defer c.mu.Unlock()
	logger.L().Debug("server: updating registered server instance", logger.Component("transport"))
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
		logger.L().Error("transport: failed to start in-memory transport because server is not registered", logger.Component("transport"))
		return fmt.Errorf("in-memory server not registered")
	}
	c.mu.Unlock()

	logger.L().Info("transport: started in-memory transport", logger.Component("transport"))
	<-ctx.Done()
	logger.L().Info("transport: shutting down in-memory transport on context cancellation", logger.Component("transport"))
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

	logger.L().Info("transport: closing in-memory transport connection and resources", logger.Component("transport"))

	if conn != nil {
		logger.L().Debug("client: closing client connection", logger.Component("transport"))
		_ = conn.Close()
	}
	if grpcServer != nil {
		logger.L().Debug("grpc: gracefully stopping grpc server", logger.Component("transport"))
		grpcServer.GracefulStop()
	}
	if lis != nil {
		logger.L().Debug("listener: closing listener", logger.Component("transport"))
		_ = lis.Close()
	}
	if socketPath != "" {
		logger.L().Debug("socket: removing socket file", logger.Component("transport"), logger.String("path", socketPath))
		_ = os.Remove(socketPath)
	}
}

// DoAction processes the action via the underlying FlightClient.
func (c *InMemory) DoAction(ctx context.Context, action *Action) (ResultStream, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	if client == nil {
		logger.L().Warn("client: in-memory client is closed, action rejected", logger.Component("transport"), logger.String("action_type", action.Type))
		return nil, fmt.Errorf("in-memory client is closed")
	}
	logger.L().Debug("client: executing action via in-memory client", logger.Component("transport"), logger.String("action_type", action.Type))
	res, err := client.DoAction(ctx, action)
	if err != nil {
		logger.L().Error("client: action execution failed", logger.Component("transport"), logger.String("action_type", action.Type), logger.Error(err))
		return nil, err
	}
	return res, nil
}

// inMemoryExchangeStream wraps flightExchangeStream to provide a Close method.
type inMemoryExchangeStream struct {
	*flightExchangeStream
}

// Close closes the send side of the stream.
func (s *inMemoryExchangeStream) Close() error {
	logger.L().Debug("stream: closing in-memory exchange stream send side", logger.Component("transport"))
	err := s.stream.CloseSend()
	if err != nil {
		logger.L().Warn("stream: failed to close in-memory exchange stream send side", logger.Component("transport"), logger.Error(err))
	}
	return err
}

// DoExchange opens an in-memory exchange stream pairing a client stream and a server stream.
func (c *InMemory) DoExchange(ctx context.Context) (ExchangeStream, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	if client == nil {
		logger.L().Warn("client: in-memory client is closed, exchange rejected", logger.Component("transport"))
		return nil, fmt.Errorf("in-memory client is closed")
	}

	logger.L().Debug("client: opening bidirectional exchange stream", logger.Component("transport"))
	stream, err := client.DoExchange(ctx)
	if err != nil {
		logger.L().Error("client: exchange stream setup failed", logger.Component("transport"), logger.Error(err))
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
		logger.L().Error("server: in-memory server not registered for action", logger.Component("transport"), logger.String("action_type", action.Type))
		return fmt.Errorf("in-memory server not registered")
	}
	logger.L().Debug("server: proxying action request to registered server", logger.Component("transport"), logger.String("action_type", action.Type))
	return srv.DoAction(ctx, action, stream)
}

func (p *serverProxy) DoExchange(ctx context.Context, stream ExchangeStream) error {
	srv := p.inMemory.getServer()
	if srv == nil {
		logger.L().Error("server: in-memory server not registered for exchange", logger.Component("transport"))
		return fmt.Errorf("in-memory server not registered")
	}
	logger.L().Debug("server: proxying exchange stream to registered server", logger.Component("transport"))
	return srv.DoExchange(ctx, stream)
}
