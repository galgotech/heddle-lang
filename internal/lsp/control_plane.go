package lsp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/internal/models"
)

// ControlPlaneLSPClient handles communication with the Heddle Control Plane.
type ControlPlaneLSPClient struct {
	addr   string
	mu     sync.RWMutex
	conn   *grpc.ClientConn
	flight flight.Client
}

// IsConnected returns true if the client is currently connected.
func (c *ControlPlaneLSPClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil
}

// Connect establishes a connection to the control plane.
func (c *ControlPlaneLSPClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil // Already connected, no error
	}

	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c.conn = conn
	c.flight = flight.NewClientFromConn(conn, nil)
	return nil
}

// GetRegistry fetches the step registry from the control plane.
func (c *ControlPlaneLSPClient) GetRegistry(ctx context.Context) (*models.RegistryInfo, error) {
	c.mu.RLock()
	client := c.flight
	connected := c.conn != nil
	c.mu.RUnlock()

	if !connected {
		return nil, fmt.Errorf("not connected to control plane")
	}

	res, err := client.DoAction(ctx, &flight.Action{
		Type: models.ActionGetRegistry,
	})
	if err != nil {
		return nil, err
	}

	resp, err := res.Recv()
	if err != nil {
		return nil, err
	}

	var info models.RegistryInfo
	if err := json.Unmarshal(resp.Body, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

// Close closes the connection to the control plane.
func (c *ControlPlaneLSPClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.flight = nil
		return err
	}
	return nil
}

// NewControlPlaneLSPClient creates a new client for the given address.
func NewControlPlaneLSPClient(addr string) *ControlPlaneLSPClient {
	return &ControlPlaneLSPClient{addr: addr}
}
