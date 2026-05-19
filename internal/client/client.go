package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/internal/models"
)

type ControlPlaneClient struct {
	ctx  context.Context
	addr string

	client flight.Client
	stream flight.FlightService_DoExchangeClient
}

func (c *ControlPlaneClient) connect() error {
	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to control plane: %w", err)
	}

	c.client = flight.NewClientFromConn(conn, nil)
	c.stream, err = c.client.DoExchange(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	return nil
}

func (c *ControlPlaneClient) SubmitWorkflow(source string, workflowName string, async bool) (string, error) {
	sub := models.WorkflowSubmission{
		Source:       source,
		WorkflowName: workflowName,
		Strategy:     "recursive",
	}

	body, err := c.sendAction(c.ctx, models.ActionSubmitWorkflow, sub)
	if err != nil {
		return "", fmt.Errorf("failed to marshal submission: %w", err)
	}
	if async {
		return string(body), nil
	}

	for {
		// iteractive communication with controleplane
		rec, err := c.stream.Recv()
		if err != nil {
			return "", fmt.Errorf("failed to receive submission result: %w", err)
		}
		fmt.Println(rec)

	}

	return "", nil
}

func (c *ControlPlaneClient) sendAction(ctx context.Context, actionType string, sub models.WorkflowSubmission) (string, error) {
	body, err := json.Marshal(sub)
	if err != nil {
		return "", fmt.Errorf("failed to marshal submission: %w", err)
	}

	res, err := c.client.DoAction(ctx, &flight.Action{
		Type: actionType,
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

func NewControlPlaneClient(ctx context.Context, addr string) (*ControlPlaneClient, error) {
	metaData, ok := metadata.FromOutgoingContext(ctx)
	if !ok || (len(metaData.Get("client-id")) == 0 && len(metaData.Get("worker-id")) == 0) {
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", "client-"+uuid.New().String()[:8])
	}
	client := &ControlPlaneClient{
		ctx:  ctx,
		addr: addr,
	}
	client.connect()
	return client, nil
}
