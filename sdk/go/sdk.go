package heddlesdk

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PluginClient represents a Heddle plugin client that connects to the control plane.
type PluginClient struct {
	ID     string
	Addr   string
	Client flight.Client
	conn   *grpc.ClientConn
}

// NewPluginClient creates a new Heddle plugin client.
func NewPluginClient(id, addr string) (*PluginClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to plugin server: %w", err)
	}

	client := flight.NewClientFromConn(conn, nil)

	return &PluginClient{
		ID:     id,
		Addr:   addr,
		Client: client,
		conn:   conn,
	}, nil
}

// Close closes the connection to the control plane.
func (c *PluginClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Run starts the plugin execution loop.
func (c *PluginClient) Run(ctx context.Context) error {
	log.Printf("Plugin %s connecting to server at %s", c.ID, c.Addr)

	// Open exchange stream for tasks
	stream, err := c.Client.DoExchange(ctx)
	if err != nil {
		return fmt.Errorf("failed to open exchange: %w", err)
	}

	log.Printf("Plugin %s execution loop started", c.ID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Recv will block until a task is received or the connection is closed
			_, err := stream.Recv()
			if err != nil {
				log.Printf("Server connection closed or error: %v", err)
				return err
			}

			// TODO: Implement task routing and execution logic
			log.Printf("Plugin %s received a task", c.ID)
		}
	}
}
