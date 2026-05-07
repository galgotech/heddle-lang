package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/data"
	"github.com/galgotech/heddle-lang/pkg/runtime/transport"
	"github.com/galgotech/heddle-lang/sdk/go/proto"
)

// Worker represents a high-performance orchestration node responsible for task execution.
// It coordinates with the Control Plane via a NetworkTransport abstraction and delegates
// intensive computation to polyglot plugins using zero-copy memory handles.
type Worker struct {
	ID        string
	Transport transport.NetworkTransport
	dataMgr   data.DataManager
	udsServer *data.UDSServer
	udsAddr   string

	// Plugin management
	pm *PluginManager
	
	// Batching aggregator
	agg *Aggregator

	// Flight server for P2P
	flight.BaseFlightServer
}

// NewWorker initializes a new worker instance using the provided transport and data manager.
func NewWorker(id string, trans transport.NetworkTransport, dataMgr data.DataManager, batchSize int, batchWindow time.Duration) *Worker {
	udsAddr := fmt.Sprintf("/tmp/heddle-%s.sock", id)

	w := &Worker{
		ID:        id,
		Transport: trans,
		dataMgr:   dataMgr,
		udsServer: data.NewUDSServer(udsAddr, dataMgr),
		udsAddr:   udsAddr,
		pm:        NewPluginManager(),
	}

	// Initialize aggregator with provided config
	config := BatchConfig{
		MaxBatchSize: batchSize,
		TimeWindow:   batchWindow,
	}
	w.agg = NewAggregator(config, memory.DefaultAllocator, w.batchExecutor)

	return w
}


// Register notifies the Control Plane of the worker's availability.
func (w *Worker) Register(ctx context.Context) error {
	reg := WorkerRegistration{
		WorkerID:   w.ID,
		Address:    "localhost:0",
		UDSAddress: w.udsAddr,
		Runtime:    "go",
	}

	body, _ := json.Marshal(reg)
	action := &flight.Action{
		Type: ActionRegisterWorker,
		Body: body,
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "x-heddle-worker-id", w.ID)
	stream, err := w.Transport.DoAction(ctx, action)
	if err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}

	_, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive registration result: %w", err)
	}

	logger.L().Info("Worker registered successfully", logger.String("workerID", w.ID))
	return nil
}

// StartHeartbeat maintains the worker's liveness state.
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
			stream, err := w.Transport.DoAction(hbCtx, action)
			if err != nil {
				logger.L().Warn("Heartbeat failed", logger.Error(err))
				continue
			}
			_, _ = stream.Recv()
		case <-ctx.Done():
			return
		}
	}
}

// StartExecutionLoop processes tasks from the Control Plane concurrently to allow batching.
func (w *Worker) StartExecutionLoop(ctx context.Context) {
	exCtx := metadata.AppendToOutgoingContext(ctx, "x-heddle-worker-id", w.ID)
	stream, err := w.Transport.DoExchange(exCtx)
	if err != nil {
		logger.L().Fatal("failed to open exchange stream", logger.Error(err))
	}

	logger.L().Info("Worker execution loop started", logger.String("workerID", w.ID))

	// Channel to coordinate task updates back to the control plane
	updateCh := make(chan TaskUpdate, 100)

	// Update sender goroutine
	go func() {
		for {
			select {
			case update := <-updateCh:
				updateBody, _ := json.Marshal(update)
				if err := stream.Send(&flight.FlightData{DataBody: updateBody}); err != nil {
					logger.L().Error("Failed to send task update", logger.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
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

			// Process tasks in parallel to enable the Aggregator to converge structural overlaps
			go func(t Task) {
				logger.L().Info("Executing task",
					logger.String("taskID", t.ID),
					logger.String("step", t.Step.DefinitionName))

				outputHandle, err := w.executeTask(ctx, t)

				update := TaskUpdate{
					TaskID:       t.ID,
					Status:       string(TaskStatusDone),
					OutputHandle: outputHandle,
					Timestamp:    time.Now(),
				}
				if err != nil {
					update.Status = string(TaskStatusFailed)
					update.Error = err.Error()
				}
				updateCh <- update
			}(task)
		}
	}
}

// executeTask manages the full lifecycle of a task, utilizing the Aggregator for execution affinity.
func (w *Worker) executeTask(ctx context.Context, task Task) (string, error) {
	if task.Step == nil || len(task.Step.Call) < 2 {
		return "", fmt.Errorf("step implementation mapping invalid: %v", task.Step)
	}

	for _, ticket := range task.Tickets {
		if ticket.RouteType == proto.RouteType_REMOTE {
			_, err := w.fetchRemoteData(ctx, ticket)
			if err != nil {
				return "", fmt.Errorf("failed to fetch remote data for %s: %w", ticket.ResourceId, err)
			}
		}
	}

	// 1. Identify function signature for batching
	signature := strings.Join(task.Step.Call, "|")

	// 2. Extract primary input record
	var inputHandle string
	for _, ticket := range task.Tickets {
		inputHandle = ticket.ResourceId
	}
	
	rec, err := w.dataMgr.Get(inputHandle)
	if err != nil {
		return "", fmt.Errorf("input handle %s not found in DataManager: %w", inputHandle, err)
	}

	// 3. Intercept execution via Aggregator
	promise := w.agg.Add(signature, rec)

	select {
	case resRec := <-promise.ResCh:
		// 4. Generate deterministic handle for result
		outputHandle := fmt.Sprintf("shm-%s-%d", task.ID, time.Now().UnixNano())
		if err := w.dataMgr.Put(outputHandle, resRec); err != nil {
			return "", fmt.Errorf("failed to store result: %w", err)
		}
		return outputHandle, nil
	case err := <-promise.ErrCh:
		return "", err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// batchExecutor handles the actual invocation of merged Arrow tables.
func (w *Worker) batchExecutor(ctx context.Context, signature string, table arrow.Table) (arrow.Table, error) {
	parts := strings.Split(signature, "|")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid signature format: %s", signature)
	}
	module, name := parts[0], parts[1]

	// Convert Table to a temporary Record for DataManager registration
	tr := array.NewTableReader(table, table.NumRows())
	defer tr.Release()
	if !tr.Next() {
		return nil, fmt.Errorf("failed to read table records")
	}
	rec := tr.Record()

	inputHandle := fmt.Sprintf("batch-%s-%d", signature, time.Now().UnixNano())
	if err := w.dataMgr.Put(inputHandle, rec); err != nil {
		return nil, err
	}

	outputHandle, err := w.delegateToPlugin(ctx, module, name, inputHandle)
	if err != nil {
		return nil, err
	}

	// Import the output handle to register it in the worker's DataManager
	if err := w.dataMgr.Import(outputHandle); err != nil {
		return nil, fmt.Errorf("failed to import batch output %s: %w", outputHandle, err)
	}

	resRec, err := w.dataMgr.Get(outputHandle)
	if err != nil {
		return nil, fmt.Errorf("output handle %s not found: %w", outputHandle, err)
	}

	return array.NewTableFromRecords(resRec.Schema(), []arrow.Record{resRec}), nil
}

// fetchRemoteData pulls an Arrow RecordBatch from a remote peer.
func (w *Worker) fetchRemoteData(ctx context.Context, ticket *proto.FlightTicket) (string, error) {
	addr := strings.TrimPrefix(ticket.Address, "grpc://")

	// Ideally, this should also be abstracted via a transport factory to avoid direct gRPC dependency.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("failed to connect to peer %s: %w", addr, err)
	}
	defer conn.Close()

	peerTransport := transport.NewFlightTransport(conn)
	stream, err := peerTransport.DoGet(ctx, &flight.Ticket{Ticket: []byte(ticket.ResourceId)})
	if err != nil {
		return "", fmt.Errorf("DoGet failed for %s: %w", ticket.ResourceId, err)
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return "", fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		return "", fmt.Errorf("no data received from peer")
	}

	rec := reader.Record()
	localHandle := fmt.Sprintf("remote-%s-%d", ticket.ResourceId, time.Now().UnixNano())
	if err := w.dataMgr.Put(localHandle, rec); err != nil {
		return "", fmt.Errorf("failed to store remote data locally: %w", err)
	}

	return localHandle, nil
}


// delegateToPlugin transmits execution instructions to a polyglot plugin using SCM_RIGHTS.
func (w *Worker) delegateToPlugin(ctx context.Context, module string, name string, inputHandle string) (string, error) {
	// The module part is now treated as the plugin's namespace.
	namespace := module
	if parts := strings.Split(module, ":"); len(parts) > 1 {
		namespace = parts[1]
	}

	// 1. Resolve an active UDS connection to the target plugin namespace host.
	plugin, ok := w.pm.GetPlugin(namespace)
	if !ok {
		baseDir := os.Getenv("HEDDLE_PLUGIN_SOCKET_DIR")
		if baseDir == "" {
			baseDir = "/tmp"
		}
		addr := fmt.Sprintf("unix://%s/heddle-plugin-%s.sock", baseDir, namespace)
		var err error
		plugin, err = w.pm.ConnectPlugin(ctx, namespace, addr)
		if err != nil {
			return "", fmt.Errorf("failed to connect to %s plugin: %w", namespace, err)
		}
	}

	// 2. Retrieve the underlying File Descriptor (FD) from the DataManager registry.
	file := w.dataMgr.GetRegistry().GetFile(inputHandle)
	if file == nil {
		return "", fmt.Errorf("input handle %s not found in DataManager registry", inputHandle)
	}

	// 3. Generate a deterministic handle for the output RecordBatch.
	outputHandle := fmt.Sprintf("shm-out-%d", time.Now().UnixNano())

	// 4. Build the execution payload.
	req := &proto.ExecuteStepRequest{
		StepName:     name,
		InputHandle:  inputHandle,
		OutputHandle: outputHandle,
	}

	// 5. Execute the step via UDS.
	logger.L().Debug("Delegating task to plugin via ExecuteStep",
		logger.String("namespace", namespace),
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
