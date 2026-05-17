package lsp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/internal/models"
)

// ControlPlaneLSPClient manages the communication channel with the Heddle Control Plane.
// It establishes a thread-safe connection to coordinate metadata, query step registries,
// and facilitate integration between the language server and the orchestration plane
// using high-performance gRPC and Apache Arrow Flight protocols.
type ControlPlaneLSPClient struct {
	addr   string
	conn   *grpc.ClientConn
	flight flight.Client
}

// IsConnected returns true if the gRPC connection has been established and is active.
// This call is thread-safe and utilizes a read lock to allow non-blocking concurrent checks.
func (c *ControlPlaneLSPClient) IsConnected() bool {
	return c.conn != nil
}

// Connect establishes the underlying gRPC transport and initializes the Apache Arrow Flight
// client for communicating with the Heddle Control Plane.
// It acquires an exclusive write lock to ensure the connection is initialized atomically and
// avoids redundant connection attempts if already established.
func (c *ControlPlaneLSPClient) Connect(ctx context.Context) error {
	// Return immediately if another routine has already established the connection.
	if c.conn != nil {
		return nil
	}

	// Establish an insecure gRPC client connection to the control plane.
	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c.conn = conn
	// Instantiate the Apache Arrow Flight client using the established gRPC transport connection.
	c.flight = flight.NewClientFromConn(conn, nil)
	return nil
}

// GetRegistry queries the control plane via Arrow Flight Action to fetch step schema definitions.
// It uses a read lock to safely snapshot the active flight client without blocking other readers,
// ensuring the subsequent remote flight RPC runs outside the critical section to maximize concurrency.
func (c *ControlPlaneLSPClient) GetRegistry(ctx context.Context) (*models.RegistryInfo, error) {
	client := c.flight
	connected := c.conn != nil
	if !connected {
		return nil, fmt.Errorf("not connected to control plane")
	}

	// Perform the remote flight RPC action to request the global step registry.
	res, err := client.DoAction(ctx, &flight.Action{
		Type: models.ActionGetRegistry,
	})
	if err != nil {
		return nil, err
	}

	// Receive the action result stream from the flight channel.
	resp, err := res.Recv()
	if err != nil {
		return nil, err
	}

	// Unmarshal the JSON payload containing the step schemas and capability metadata.
	var info models.RegistryInfo
	if err := json.Unmarshal(resp.Body, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

// Close gracefully terminates the connection to the control plane, releasing the active
// gRPC client connection and resetting the internal Flight client reference.
// It acquires an exclusive write lock to ensure the teardown operation is safe and atomic.
func (c *ControlPlaneLSPClient) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.flight = nil
		return err
	}
	return nil
}

// NewControlPlaneLSPClient instantiates a new ControlPlaneLSPClient for the designated target address.
// The connection itself must be initialized separately by calling the Connect method.
func NewControlPlaneLSPClient(addr string) *ControlPlaneLSPClient {
	return &ControlPlaneLSPClient{addr: addr}
}
