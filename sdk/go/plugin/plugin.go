package plugin

import (
	"fmt"
	"net"
	"os"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
)

// Plugin wraps the registry and server to provide the user-facing API.
type Plugin struct {
	registry *Registry
	server   *Server
}

// RegisterResource is a proxy to Registry.RegisterResource.
func (p *Plugin) RegisterResource(name string, fn any) {
	p.registry.RegisterResource(name, fn)
}

// RegisterStep is a proxy to Registry.RegisterStep.
func (p *Plugin) RegisterStep(name string, fn any, opts ...StepOption) {
	p.registry.RegisterStep(name, fn, opts...)
}

// Serve starts the gRPC server on the address specified by the PORT environment variable, defaulting to 50051.
func (p *Plugin) Serve() error {
	port := os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}
	addr := ":" + port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	return p.ServeListener(lis)
}

// ServeListener starts the Arrow Flight server and UDS handler on a specific listener.
func (p *Plugin) ServeListener(lis net.Listener) error {
	// If it's a unix socket, we use a multiplexing listener to handle both gRPC and raw UDS.
	if _, ok := lis.(*net.UnixListener); ok {
		mux := &multiplexListener{Listener: lis, server: p.server}
		grpcServer := grpc.NewServer()
		flight.RegisterFlightServiceServer(grpcServer, p.server)
		return grpcServer.Serve(mux)
	}

	grpcServer := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcServer, p.server)
	return grpcServer.Serve(lis)
}

// New creates a new SDK Plugin instance with the specified namespace.
func New(namespace string) *Plugin {
	registry := NewRegistry()
	server := NewServer(registry, namespace)

	return &Plugin{
		registry,
		server,
	}
}
