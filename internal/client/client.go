package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/internal/models"
)

type ControlPlaneClient struct {
	client flight.Client
}

func (c *ControlPlaneClient) SubmitWorkflow(ctx context.Context, source string, workflowName string) (string, error) {
	sub := models.WorkflowSubmission{
		Source:       source,
		WorkflowName: workflowName,
		Strategy:     "recursive",
	}
	body, err := json.Marshal(sub)
	if err != nil {
		return "", fmt.Errorf("failed to marshal submission: %w", err)
	}

	res, err := c.client.DoAction(ctx, &flight.Action{
		Type: models.ActionSubmitWorkflow,
		Body: body,
	})
	if err != nil {
		return "", fmt.Errorf("failed to submit workflow: %w", err)
	}

	// Read results from stream
	result, err := res.Recv()
	if err != nil {
		return "", fmt.Errorf("failed to receive submission result: %w", err)
	}

	return string(result.Body), nil
}

func NewControlPlaneClient(addr string) (*ControlPlaneClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to control plane: %w", err)
	}

	return &ControlPlaneClient{
		client: flight.NewClientFromConn(conn, nil),
	}, nil
}
