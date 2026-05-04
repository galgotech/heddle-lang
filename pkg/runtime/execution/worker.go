package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	pb "google.golang.org/protobuf/proto"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/data"
	"github.com/galgotech/heddle-lang/sdk/go/proto"
)

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

func NewWorker(id, cpAddr string) (*Worker, error) {
	conn, err := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CP: %w", err)
	}

	client := flight.NewClientFromConn(conn, nil)
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

func (w *Worker) Register(ctx context.Context) error {
	reg := WorkerRegistration{
		WorkerID: w.ID,
		Address:  "localhost:0", // In a real scenario, this would be the worker's listen address
		Runtime:  "go",
	}

	body, _ := json.Marshal(reg)
	action := &flight.Action{
		Type: ActionRegisterWorker,
		Body: body,
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "x-heddle-worker-id", w.ID)
	stream, err := w.Client.DoAction(ctx, action)
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
			stream, err := w.Client.DoAction(hbCtx, action)
			if err != nil {
				logger.L().Warn("Heartbeat failed", logger.Error(err))
				continue
			}
			_, _ = stream.Recv() // Drain result
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) StartUDSServer(ctx context.Context) {
	if err := w.udsServer.Start(ctx); err != nil {
		logger.L().Error("UDS Server failed", logger.Error(err))
	}
}

func (w *Worker) StartExecutionLoop(ctx context.Context) {
	exCtx := metadata.AppendToOutgoingContext(ctx, "x-heddle-worker-id", w.ID)
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

			// Execute step
			outputHandle, err := w.executeTask(ctx, task)

			// Report update
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

			updateBody, _ := json.Marshal(update)
			if err := stream.Send(&flight.FlightData{DataBody: updateBody}); err != nil {
				logger.L().Error("Failed to send task update", logger.Error(err))
			}
		}
	}
}

func (w *Worker) StartFlightServer(ctx context.Context, addr string) error {
	server := flight.NewServerWithMiddleware(nil)
	server.RegisterFlightService(&workerFlightServer{
		Worker: w,
	})

	if err := server.Init(addr); err != nil {
		return fmt.Errorf("failed to init flight server: %w", err)
	}

	logger.L().Info("P2P Flight Server started", logger.String("addr", addr))

	go func() {
		<-ctx.Done()
		server.Shutdown()
	}()

	return server.Serve()
}

type workerFlightServer struct {
	flight.BaseFlightServer
	*Worker
}

func (s *workerFlightServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	resourceID := string(tkt.Ticket)
	logger.L().Debug("DoGet request received", logger.String("resourceID", resourceID))

	rec, err := s.dataMgr.Get(resourceID)
	if err != nil {
		return fmt.Errorf("resource not found: %w", err)
	}
	defer rec.Release()

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(rec.Schema()))
	if err := writer.Write(rec); err != nil {
		return fmt.Errorf("failed to write record to flight stream: %w", err)
	}
	return writer.Close()
}

func (w *Worker) fetchRemoteData(ctx context.Context, ticket *proto.FlightTicket) (string, error) {
	addr := strings.TrimPrefix(ticket.Address, "grpc://")

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("failed to connect to peer %s: %w", addr, err)
	}
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)
	stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: []byte(ticket.ResourceId)})
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

func (w *Worker) executeTask(ctx context.Context, task Task) (string, error) {
	if task.Step == nil || len(task.Step.Call) < 2 {
		return "", fmt.Errorf("step implementation mapping invalid: %v", task.Step)
	}

	// 1. Fetch remote data if necessary
	if task.RemoteTicket != nil && task.RemoteTicket.RouteType == proto.RouteType_REMOTE {
		localHandle, err := w.fetchRemoteData(ctx, task.RemoteTicket)
		if err != nil {
			return "", fmt.Errorf("failed to fetch remote data: %w", err)
		}
		task.InputHandle = localHandle
	}

	// 2. Delegate execution to polyglot plugin
	outputHandle, err := w.delegateTask(ctx, task)
	if err != nil {
		return "", fmt.Errorf("delegation failed: %w", err)
	}

	// 3. Register output in DataManager if it's a new handle
	if outputHandle != "" {
		if err := w.dataMgr.Import(outputHandle); err != nil {
			return "", fmt.Errorf("failed to import output handle %s: %w", outputHandle, err)
		}
	}

	return outputHandle, nil
}

func (w *Worker) delegateTask(ctx context.Context, task Task) (string, error) {
	module := task.Step.Call[0]
	name := task.Step.Call[1]

	// Determine language from module (e.g., "py:foo" or "std:io")
	lang := "go" // Default to Go plugin for stdlib
	if strings.HasPrefix(module, "py:") {
		lang = "python"
	} else if strings.HasPrefix(module, "rs:") {
		lang = "rust"
	} else if strings.HasPrefix(module, "js:") {
		lang = "node"
	}

	// 1. Get or start plugin instance to get its address
	plugin, ok := w.pm.GetPlugin(lang)
	if !ok {
		addr := fmt.Sprintf("unix:///tmp/heddle-plugin-%s.sock", lang)
		var err error
		plugin, err = w.pm.ConnectPlugin(ctx, lang, addr)
		if err != nil {
			return "", fmt.Errorf("failed to connect to %s plugin: %w", lang, err)
		}
	}

	// 2. Prepare UDS connection to plugin for proactive FD passing
	pluginUdsPath := strings.TrimPrefix(plugin.Address, "unix://")
	conn, err := net.Dial("unix", pluginUdsPath)
	if err != nil {
		return "", fmt.Errorf("failed to dial plugin UDS: %w", err)
	}
	defer conn.Close()
	unixConn := conn.(*net.UnixConn)

	// 3. Retrieve FD for input handle
	file := w.dataMgr.GetRegistry().GetFile(task.InputHandle)
	if file == nil {
		// If not in registry (e.g. from local storage), we might need to open it
		// but for this refactor we assume it's already managed by DataManager.
		return "", fmt.Errorf("input handle %s not found in DataManager registry", task.InputHandle)
	}

	// 4. Prepare output handle
	outputHandle := fmt.Sprintf("shm-%s-%d", task.ID, time.Now().UnixNano())

	// 5. Prepare and transmit metadata + FD
	req := &proto.ExecuteStepRequest{
		StepName:     name,
		InputHandle:  task.InputHandle,
		OutputHandle: outputHandle,
	}

	meta, err := pb.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	logger.L().Debug("Transmitting FD and metadata to plugin",
		logger.String("lang", lang),
		logger.String("handle", task.InputHandle))

	if err := data.SendFDWithMetadata(unixConn, int(file.Fd()), meta); err != nil {
		return "", fmt.Errorf("failed to transmit FD and metadata: %w", err)
	}

	// 6. Receive response from plugin
	respBuf := make([]byte, 4096)
	n, err := unixConn.Read(respBuf)
	if err != nil {
		return "", fmt.Errorf("failed to read response from plugin: %w", err)
	}

	var resp proto.ExecuteStepResponse
	if err := pb.Unmarshal(respBuf[:n], &resp); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if resp.Status != proto.StatusCode_SUCCESS {
		return "", fmt.Errorf("plugin error: %s", resp.ErrorMessage)
	}

	return resp.OutputHandle, nil
}
