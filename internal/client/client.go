package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/apache/arrow/go/v18/arrow"
	arrowcsv "github.com/apache/arrow/go/v18/arrow/csv"
	"github.com/apache/arrow/go/v18/arrow/ipc"

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

		if len(rec.AppMetadata) > 0 {
			var ctrl models.ControlMessage
			if err := json.Unmarshal(rec.AppMetadata, &ctrl); err == nil {
				if ctrl.Type == models.ActionRequestFile && ctrl.FileRequest != nil {
					logger.L().Info("file request received: starting direct upload to worker",
						logger.Component("client"),
						logger.String("file", ctrl.FileRequest.FilePath),
						logger.String("worker", ctrl.FileRequest.WorkerAddress),
					)
					go c.handleFileUpload(ctrl.FileRequest)
					continue
				}
			}
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
			if msg != "" {
				fmt.Println(msg)
			}
		}
	}
}

func (c *ControlPlaneClient) handleFileUpload(req *models.FileRequest) {
	if len(req.Columns) == 0 {
		logger.L().Error("file upload failed: no columns defined in configuration schema", logger.Component("client"), logger.String("file", req.FilePath))
		return
	}

	file, err := os.Open(req.FilePath)
	if err != nil {
		logger.L().Error("file upload failed: unable to open local file", logger.Component("client"), logger.String("file", req.FilePath), logger.Error(err))
		return
	}
	defer file.Close()

	// Build the schema fields sorted by column name to ensure deterministic order
	var colNames []string
	for colName := range req.Columns {
		colNames = append(colNames, colName)
	}
	sort.Strings(colNames)

	fields := make([]arrow.Field, 0, len(colNames))
	for _, colName := range colNames {
		tStr := req.Columns[colName]
		t, err := parseDataType(tStr)
		if err != nil {
			logger.L().Error("file upload failed: invalid column type definition", logger.Component("client"), logger.String("column", colName), logger.String("type", tStr), logger.Error(err))
			return
		}
		fields = append(fields, arrow.Field{
			Name:     colName,
			Type:     t,
			Nullable: true,
		})
	}
	arrowSchema := arrow.NewSchema(fields, nil)

	// Map incoming options to arrow csv options
	var opts []arrowcsv.Option
	if delimVal, ok := req.Options["delimiter"]; ok {
		if delimStr, ok := delimVal.(string); ok && len(delimStr) > 0 {
			r := []rune(delimStr)[0]
			opts = append(opts, arrowcsv.WithComma(r))
		}
	}
	if commentVal, ok := req.Options["comment"]; ok {
		if commentStr, ok := commentVal.(string); ok && len(commentStr) > 0 {
			r := []rune(commentStr)[0]
			opts = append(opts, arrowcsv.WithComment(r))
		}
	}
	if lazyVal, ok := req.Options["lazy_quotes"]; ok {
		if lazyBool, ok := lazyVal.(bool); ok {
			opts = append(opts, arrowcsv.WithLazyQuotes(lazyBool))
		}
	}
	if headerVal, ok := req.Options["has_header"]; ok {
		if headerBool, ok := headerVal.(bool); ok {
			opts = append(opts, arrowcsv.WithHeader(headerBool))
		}
	} else {
		opts = append(opts, arrowcsv.WithHeader(true))
	}

	reader := arrowcsv.NewReader(file, arrowSchema, opts...)
	defer reader.Release()

	addr := req.WorkerAddress
	if (strings.HasPrefix(addr, "/") || strings.HasPrefix(addr, "./") || strings.HasSuffix(addr, ".sock")) && !strings.Contains(addr, "://") {
		addr = "unix://" + addr
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.L().Error("file upload failed: error dialing worker", logger.Component("client"), logger.String("worker", addr), logger.Error(err))
		return
	}
	defer conn.Close()

	workerClient := flight.NewClientFromConn(conn, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "x-heddle-task-id", req.TaskID)

	stream, err := workerClient.DoPut(ctx)
	if err != nil {
		logger.L().Error("file upload failed: DoPut failed", logger.Component("client"), logger.Error(err))
		return
	}

	flightWriter := flight.NewRecordWriter(stream, ipc.WithSchema(arrowSchema))
	defer flightWriter.Close()

	for reader.Next() {
		rec := reader.Record()
		if err := flightWriter.Write(rec); err != nil {
			logger.L().Error("file upload failed: could not write record", logger.Component("client"), logger.Error(err))
			return
		}
	}

	if err := reader.Err(); err != nil && err != io.EOF {
		logger.L().Error("file upload failed: error parsing CSV data", logger.Component("client"), logger.Error(err))
		return
	}

	logger.L().Info("file upload successful: data sent to worker", logger.Component("client"), logger.String("file", req.FilePath))
}

func parseDataType(t string) (arrow.DataType, error) {
	switch strings.ToLower(t) {
	case "int8":
		return arrow.PrimitiveTypes.Int8, nil
	case "int16":
		return arrow.PrimitiveTypes.Int16, nil
	case "int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "uint8":
		return arrow.PrimitiveTypes.Uint8, nil
	case "uint16":
		return arrow.PrimitiveTypes.Uint16, nil
	case "uint32":
		return arrow.PrimitiveTypes.Uint32, nil
	case "uint64":
		return arrow.PrimitiveTypes.Uint64, nil
	case "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "string", "utf8", "text":
		return arrow.BinaryTypes.String, nil
	case "bool", "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	default:
		return nil, fmt.Errorf("unsupported target data type: %s", t)
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
