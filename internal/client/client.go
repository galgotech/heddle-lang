package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

type ControlPlaneClient struct {
	ctx  context.Context
	addr string

	client flight.Client
	stream flight.FlightService_DoExchangeClient

	In io.Reader // For reading interactive approvals, defaults to os.Stdin
}

func NewControlPlaneClient(ctx context.Context, addr string) (*ControlPlaneClient, error) {
	if (strings.HasPrefix(addr, "/") || strings.HasPrefix(addr, "./") || strings.HasSuffix(addr, ".sock")) && !strings.Contains(addr, "://") {
		addr = "unix://" + addr
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "client-id", "client-"+uuid.New().String()[:8])
	client := &ControlPlaneClient{
		ctx:  ctx,
		addr: addr,
		In:   os.Stdin,
	}
	if err := client.connect(); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *ControlPlaneClient) connect() error {
	logger.L().Debug("client connection initiated: connecting to control plane address", logger.Component("client"), logger.String("address", c.addr))
	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.L().Error("client connection failed: error dialing control plane", logger.Component("client"), logger.String("address", c.addr), logger.Error(err))
		return fmt.Errorf("failed to connect to control plane: %w", err)
	}

	c.client = flight.NewClientFromConn(conn, nil)

	// Register the client
	logger.L().Debug("client registration initiated: sending registration action to control plane", logger.Component("client"))
	_, err = c.sendAction(c.ctx, models.ActionRegisterClient, nil)
	if err != nil {
		logger.L().Error("client registration failed: failed to register client", logger.Component("client"), logger.Error(err))
		return fmt.Errorf("failed to register client: %w", err)
	}

	c.stream, err = c.client.DoExchange(c.ctx)
	if err != nil {
		logger.L().Error("client stream initialization failed: error establishing bidirectional exchange channel", logger.Component("client"), logger.Error(err))
		return fmt.Errorf("failed to create stream: %w", err)
	}

	// Start heartbeat routine
	go func() {
		logger.L().Debug("client heartbeat registered: active heartbeat loop running", logger.Component("client"))
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				hb := models.WorkerHeartbeat{
					Timestamp: time.Now(),
					Load:      0,
				}
				_, _ = c.sendAction(c.ctx, models.ActionHeartbeat, hb)
			}
		}
	}()

	logger.L().Info("client connection established: registered client and stream created successfully", logger.Component("client"), logger.String("address", c.addr))
	return nil
}

// SubmitWorkflowDirect submits a workflow to the control plane without consuming the interactive stream.
// This is primarily used by the DAP server which manages its own stream reader loop.
func (c *ControlPlaneClient) SubmitWorkflowDirect(source string, workflowName string, strategy string) (string, error) {
	logger.L().Debug("workflow submission initiated: direct submission request received",
		logger.Component("client"),
		logger.String("workflow", workflowName),
		logger.String("strategy", strategy),
	)
	sub := models.WorkflowSubmission{
		Source:       source,
		WorkflowName: workflowName,
		Strategy:     strategy,
		Async:        false,
	}

	res, err := c.sendAction(c.ctx, models.ActionSubmitWorkflow, sub)
	if err != nil {
		logger.L().Error("workflow submission failed: direct submission failed",
			logger.Component("client"),
			logger.String("workflow", workflowName),
			logger.Error(err),
		)
		return "", err
	}
	logger.L().Info("workflow submission completed: direct submission succeeded",
		logger.Component("client"),
		logger.String("workflow", workflowName),
	)
	return res, nil
}

func (c *ControlPlaneClient) SubmitWorkflow(source string, workflowName string, strategy string, async bool) (string, error) {
	logger.L().Debug("workflow submission initiated: submission request received",
		logger.Component("client"),
		logger.String("workflow", workflowName),
		logger.String("strategy", strategy),
		logger.Any("async", async),
	)
	sub := models.WorkflowSubmission{
		Source:       source,
		WorkflowName: workflowName,
		Strategy:     strategy,
		Async:        async,
	}

	body, err := c.sendAction(c.ctx, models.ActionSubmitWorkflow, sub)
	if err != nil {
		logger.L().Error("workflow submission failed: submission failed",
			logger.Component("client"),
			logger.String("workflow", workflowName),
			logger.Error(err),
		)
		return "", fmt.Errorf("failed to marshal submission: %w", err)
	}
	if async {
		logger.L().Info("workflow submission completed: async submission succeeded",
			logger.Component("client"),
			logger.String("workflow", workflowName),
		)
		return string(body), nil
	}

	logger.L().Info("workflow monitoring started: listening to execution progress",
		logger.Component("client"),
		logger.String("workflow", workflowName),
	)

	for {
		// interactive communication with control plane
		rec, err := c.stream.Recv()
		if err != nil {
			// EOF or connection closed is expected when the task reaches a terminal state
			logger.L().Info("workflow monitoring completed: stream closed or execution finished",
				logger.Component("client"),
				logger.String("workflow", workflowName),
			)
			return "SUCCESS", nil
		}

		msg := string(rec.DataBody)
		if after, ok := strings.CutPrefix(msg, "LOG:"); ok {
			logMsg := after
			fmt.Println(logMsg)
			if logMsg == "Workflow completed successfully." {
				logger.L().Info("workflow execution succeeded: workflow finished successfully",
					logger.Component("client"),
					logger.String("workflow", workflowName),
				)
				return "SUCCESS", nil
			}
			if strings.HasPrefix(logMsg, "Workflow failed:") {
				logger.L().Error("workflow execution failed: workflow run failed",
					logger.Component("client"),
					logger.String("workflow", workflowName),
					logger.String("reason", logMsg),
				)
				return "", fmt.Errorf("%s", logMsg)
			}

		} else if after0, ok0 := strings.CutPrefix(msg, "PROMPT:"); ok0 {
			parts := strings.Split(after0, ":")
			stepID := parts[0]
			capability := ""
			if len(parts) > 1 {
				capability = parts[1]
			}
			logger.L().Info("workflow prompt received: user approval requested",
				logger.Component("client"),
				logger.String("step", stepID),
				logger.Capability(capability),
			)
			fmt.Printf("Execute step '%s' (%s)? [y/N]: ", stepID, capability)
			var response string
			_, err := fmt.Fscanln(c.In, &response)
			if err != nil && err.Error() != "unexpected newline" {
				logger.L().Warn("workflow prompt canceled: failed to read response",
					logger.Component("client"),
					logger.String("step", stepID),
					logger.Error(err),
				)
				c.stream.Send(&flight.FlightData{DataBody: []byte("REJECT")})
				return "", fmt.Errorf("failed to read user input: %w", err)
			}
			response = strings.TrimSpace(strings.ToLower(response))
			if response == "y" || response == "yes" {
				logger.L().Info("workflow prompt resolved: step execution approved",
					logger.Component("client"),
					logger.String("step", stepID),
				)
				err = c.stream.Send(&flight.FlightData{DataBody: []byte("APPROVE")})
				if err != nil {
					logger.L().Error("workflow approval failed: error sending APPROVE message",
						logger.Component("client"),
						logger.String("step", stepID),
						logger.Error(err),
					)
					return "", fmt.Errorf("failed to send approval: %w", err)
				}
			} else {
				logger.L().Warn("workflow prompt resolved: step execution rejected by user",
					logger.Component("client"),
					logger.String("step", stepID),
				)
				err = c.stream.Send(&flight.FlightData{DataBody: []byte("REJECT")})
				if err != nil {
					logger.L().Error("workflow rejection failed: error sending REJECT message",
						logger.Component("client"),
						logger.String("step", stepID),
						logger.Error(err),
					)
					return "", fmt.Errorf("failed to send rejection: %w", err)
				}
				return "", fmt.Errorf("step execution rejected by user")
			}

		} else {
			fmt.Println(msg)
		}
	}
}

func (c *ControlPlaneClient) sendAction(ctx context.Context, actionType string, payload interface{}) (string, error) {
	var body []byte
	var err error
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			return "", fmt.Errorf("failed to marshal payload: %w", err)
		}
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

func (c *ControlPlaneClient) GetStream() flight.FlightService_DoExchangeClient {
	return c.stream
}
