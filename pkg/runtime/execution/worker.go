package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/data"
	"github.com/galgotech/heddle-lang/sdk/go/proto"
)

// Worker represents a high-performance orchestration node responsible for task execution.
// It coordinates with the Control Plane via Arrow Flight and delegates intensive
// computation to polyglot plugins using zero-copy memory handles via UDS.
type Worker struct {
	ID     string
	CPAddr string
	Client flight.Client
	conn   *grpc.ClientConn

	dataMgr   *data.DataManager
	udsServer *data.UDSServer
	udsAddr   string

	// Plugin management
	pm *PluginManager

	// Flight server for P2P
	flight.BaseFlightServer
}

// NewWorker initializes a new worker instance, establishing the Control Plane connection
// and setting up the local shared memory DataManager and UDS server for locality-aware routing.
func NewWorker(id, cpAddr string) (*Worker, error) {
	// Establish a gRPC connection to the Control Plane (Smart Control Plane).
	conn, err := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CP: %w", err)
	}

	// Initialize the Arrow Flight client for control signaling and data coordination.
	client := flight.NewClientFromConn(conn, nil)

	// Configure the DataManager to use /dev/shm for zero-copy memory mapping.
	dataMgr := data.NewDataManager("/dev/shm/heddle", 1<<30) // 1GB limit
	udsAddr := fmt.Sprintf("/tmp/heddle-%s.sock", id)

	return &Worker{
		ID:        id,
		CPAddr:    cpAddr,
		Client:    client,
		conn:      conn,
		dataMgr:   dataMgr,
		udsServer: data.NewUDSServer(udsAddr, dataMgr),
		udsAddr:   udsAddr,
		pm:        NewPluginManager(),
	}, nil
}

// Register notifies the Control Plane of the worker's availability. It transmits
// the worker's unique identity and local communication addresses (UDS) to
// facilitate locality-aware task scheduling.
func (w *Worker) Register(ctx context.Context) error {
	reg := WorkerRegistration{
		WorkerID:   w.ID,
		Address:    "localhost:0", // In a real scenario, this would be the worker's listen address
		UDSAddress: w.udsAddr,
		Runtime:    "go",
	}

	body, _ := json.Marshal(reg)
	// Use Arrow Flight DoAction for the registration handshake.
	action := &flight.Action{
		Type: ActionRegisterWorker,
		Body: body,
	}

	// Inject worker ID into gRPC metadata for server-side traceability.
	ctx = metadata.AppendToOutgoingContext(ctx, "x-heddle-worker-id", w.ID)
	stream, err := w.Client.DoAction(ctx, action)
	if err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}

	// Wait for acknowledgement from the Control Plane.
	_, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive registration result: %w", err)
	}

	logger.L().Info("Worker registered successfully", logger.String("workerID", w.ID))
	return nil
}

// StartHeartbeat maintains the worker's liveness state in the Control Plane.
// It sends periodic updates every 5 seconds to prevent registration expiration.
func (w *Worker) StartHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hb := Heartbeat{
				WorkerID:  w.ID,
				Timestamp: time.Now(),
				Status:    WorkerStatusIdle,
			}
			body, _ := json.Marshal(hb)
			action := &flight.Action{
				Type: ActionHeartbeat,
				Body: body,
			}

			hbCtx := metadata.AppendToOutgoingContext(ctx, "x-heddle-worker-id", w.ID)
			// Send heartbeat via Flight DoAction to signal liveness and current idle status.
			stream, err := w.Client.DoAction(hbCtx, action)
			if err != nil {
				logger.L().Warn("Heartbeat failed", logger.Error(err))
				continue
			}
			_, _ = stream.Recv() // Block until the Control Plane acknowledges the update.
		case <-ctx.Done():
			return
		}
	}
}

// StartExecutionLoop opens a long-lived bidirectional Flight stream (DoExchange)
// with the Control Plane. It pulls tasks and pushes execution status updates.
func (w *Worker) StartExecutionLoop(ctx context.Context) {
	exCtx := metadata.AppendToOutgoingContext(ctx, "x-heddle-worker-id", w.ID)
	// Use DoExchange for a low-latency, persistent bidirectional control channel.
	stream, err := w.Client.DoExchange(exCtx)
	if err != nil {
		logger.L().Fatal("failed to open exchange stream", logger.Error(err))
	}

	logger.L().Info("Worker execution loop started", logger.String("workerID", w.ID))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Block until the Control Plane dispatches a new Task.
			data, err := stream.Recv()
			if err != nil {
				logger.L().Info("Execution stream closed", logger.Error(err))
				return
			}

			var task Task
			if err := json.Unmarshal(data.DataBody, &task); err != nil {
				logger.L().Error("Failed to unmarshal task", logger.Error(err))
				continue
			}

			logger.L().Info("Executing task",
				logger.String("taskID", task.ID),
				logger.String("step", task.Step.DefinitionName))

			// executeTask coordinates data resolution and plugin delegation.
			outputHandle, err := w.executeTask(ctx, task)

			// Prepare the task completion update with the resulting data handle.
			update := TaskUpdate{
				TaskID:       task.ID,
				Status:       string(TaskStatusDone),
				OutputHandle: outputHandle,
				Timestamp:    time.Now(),
			}
			if err != nil {
				update.Status = string(TaskStatusFailed)
				update.Error = err.Error()
			}

			// Report the execution result back to the Control Plane.
			updateBody, _ := json.Marshal(update)
			if err := stream.Send(&flight.FlightData{DataBody: updateBody}); err != nil {
				logger.L().Error("Failed to send task update", logger.Error(err))
			}
		}
	}
}

// executeTask manages the full lifecycle of a task:
// 1. Resolving remote data dependencies via Flight DoGet.
// 2. Delegating to the polyglot plugin using zero-copy FD passing.
// 3. Registering the output handle in the local DataManager.
func (w *Worker) executeTask(ctx context.Context, task Task) (string, error) {
	if task.Step == nil || len(task.Step.Call) < 2 {
		return "", fmt.Errorf("step implementation mapping invalid: %v", task.Step)
	}

	// 1. Resolve remote data dependencies. If a ticket is marked as REMOTE,
	// pull it into local shared memory to facilitate zero-copy processing.
	for _, ticket := range task.Tickets {
		if ticket.RouteType == proto.RouteType_REMOTE {
			_, err := w.fetchRemoteData(ctx, ticket)
			if err != nil {
				return "", fmt.Errorf("failed to fetch remote data for %s: %w", ticket.ResourceId, err)
			}
		}
	}

	// 2. Delegate execution to the target language runtime (Go, Python, etc.) via UDS.
	outputHandle, err := w.delegateTask(ctx, task)
	if err != nil {
		return "", fmt.Errorf("delegation failed: %w", err)
	}

	// 3. Register the output handle in the DataManager to make it accessible
	// for downstream consumers via locality-aware routing.
	if outputHandle != "" {
		if err := w.dataMgr.Import(outputHandle); err != nil {
			return "", fmt.Errorf("failed to import output handle %s: %w", outputHandle, err)
		}
	}

	return outputHandle, nil
}

// fetchRemoteData pulls an Arrow RecordBatch from a remote peer using Arrow Flight DoGet.
// The data is persisted in local shared memory (/dev/shm) to enable zero-copy
// access for subsequent execution steps on this host.
func (w *Worker) fetchRemoteData(ctx context.Context, ticket *proto.FlightTicket) (string, error) {
	// Strip gRPC prefix to normalize the target address.
	addr := strings.TrimPrefix(ticket.Address, "grpc://")

	// Connect to the remote worker holding the source data.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("failed to connect to peer %s: %w", addr, err)
	}
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)
	// Request the data batch from the peer via its resource ID.
	stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: []byte(ticket.ResourceId)})
	if err != nil {
		return "", fmt.Errorf("DoGet failed for %s: %w", ticket.ResourceId, err)
	}

	// Initialize a RecordReader to deserialize the incoming Flight stream.
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return "", fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		return "", fmt.Errorf("no data received from peer")
	}

	rec := reader.Record()
	// Store the record in shared memory under a temporary remote handle.
	localHandle := fmt.Sprintf("remote-%s-%d", ticket.ResourceId, time.Now().UnixNano())
	if err := w.dataMgr.Put(localHandle, rec); err != nil {
		return "", fmt.Errorf("failed to store remote data locally: %w", err)
	}

	return localHandle, nil
}

// delegateTask identifies the target runtime and transmits execution instructions
// to a polyglot plugin using SCM_RIGHTS for zero-copy file descriptor (FD) passing.
func (w *Worker) delegateTask(ctx context.Context, task Task) (string, error) {
	module := task.Step.Call[0]
	name := task.Step.Call[1]

	// Map the module prefix (e.g., "py:", "rs:") to the corresponding SDK runtime.
	lang := "go"
	if strings.HasPrefix(module, "py:") {
		lang = "python"
	} else if strings.HasPrefix(module, "rs:") {
		lang = "rust"
	} else if strings.HasPrefix(module, "js:") {
		lang = "node"
	}

	// 1. Resolve an active UDS connection to the target plugin runtime host.
	plugin, ok := w.pm.GetPlugin(lang)
	if !ok {
		addr := fmt.Sprintf("unix:///tmp/heddle-plugin-%s.sock", lang)
		var err error
		plugin, err = w.pm.ConnectPlugin(ctx, lang, addr)
		if err != nil {
			return "", fmt.Errorf("failed to connect to %s plugin: %w", lang, err)
		}
	}

	// 2. Extract the primary input handle from the task's data tickets.
	var inputHandle string
	for _, ticket := range task.Tickets {
		inputHandle = ticket.ResourceId
	}

	// 3. Retrieve the underlying File Descriptor (FD) from the DataManager registry.
	// This FD is passed to the plugin to enable direct memory mapping (mmap) of the data.
	file := w.dataMgr.GetRegistry().GetFile(inputHandle)
	if file == nil {
		return "", fmt.Errorf("input handle %s not found in DataManager registry", inputHandle)
	}

	// 4. Generate a deterministic handle for the output RecordBatch.
	outputHandle := fmt.Sprintf("shm-%s-%d", task.ID, time.Now().UnixNano())

	// 5. Build the execution payload containing the function name and data handles.
	req := &proto.ExecuteStepRequest{
		StepName:     name,
		InputHandle:  inputHandle,
		OutputHandle: outputHandle,
	}

	// 6. Execute the step via UDS. The input file descriptor is passed out-of-band
	// using SCM_RIGHTS, ensuring zero-copy access for the plugin.
	logger.L().Debug("Delegating task to plugin via ExecuteStep",
		logger.String("lang", lang),
		logger.String("step", name),
		logger.String("input", inputHandle))

	resp, err := plugin.ExecuteStep(ctx, req, int(file.Fd()))
	if err != nil {
		return "", fmt.Errorf("plugin execution failed: %w", err)
	}

	if resp.Status != proto.StatusCode_SUCCESS {
		return "", fmt.Errorf("plugin error: %s", resp.ErrorMessage)
	}

	return resp.OutputHandle, nil
}
