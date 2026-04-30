package heddlesdk

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
)

// ControlPlaneClient represents a client that interacts with the Heddle control plane.
type ControlPlaneClient struct {
	Addr   string
	Client flight.Client
	conn   *grpc.ClientConn
}

// NewControlPlaneClient creates a new Heddle control plane client.
func NewControlPlaneClient(addr string) (*ControlPlaneClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to control plane: %w", err)
	}

	client := flight.NewClientFromConn(conn, nil)

	return &ControlPlaneClient{
		Addr:   addr,
		Client: client,
		conn:   conn,
	}, nil
}

// Close closes the connection to the control plane.
func (c *ControlPlaneClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// SubmitWorkflow sends a Heddle workflow file to the control plane for processing.
func (c *ControlPlaneClient) SubmitWorkflow(ctx context.Context, workflow []byte) (string, error) {
	action := &flight.Action{
		Type: execution.ActionSubmitWorkflow,
		Body: workflow,
	}

	stream, err := c.Client.DoAction(ctx, action)
	if err != nil {
		return "", fmt.Errorf("failed to submit workflow: %w", err)
	}

	result, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("failed to receive submission result: %w", err)
	}

	return string(result.Body), nil
}

// GetHistory retrieves the execution history for the active workflow.
func (c *ControlPlaneClient) GetHistory(ctx context.Context) ([]execution.TaskUpdate, error) {
	action := &flight.Action{
		Type: execution.ActionGetHistory,
	}

	stream, err := c.Client.DoAction(ctx, action)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch history: %w", err)
	}

	result, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive history result: %w", err)
	}

	var history []execution.TaskUpdate
	if err := json.Unmarshal(result.Body, &history); err != nil {
		return nil, fmt.Errorf("failed to unmarshal history: %w", err)
	}

	return history, nil
}

// GetHeddleFramePreview fetches a JSON representation of the first few rows of a HeddleFrame.
func (c *ControlPlaneClient) GetHeddleFramePreview(ctx context.Context, handle string) (string, error) {
	// In a real implementation, this would connect to the DataManager
	// where the handle is located. For now, we simulate or use a default DM.

	// Implementation placeholder: returning a mock JSON for now
	return "[{\"id\": 1, \"name\": \"example\"}, {\"id\": 2, \"name\": \"test\"}]", nil
}
