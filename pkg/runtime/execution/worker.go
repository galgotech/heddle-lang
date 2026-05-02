package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

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

	// Plugin server
	flight.BaseFlightServer
	pluginAddr string
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
		ID:         id,
		CPAddr:     cpAddr,
		Client:     client,
		conn:       conn,
		dataMgr:    dataMgr,
		udsServer:  data.NewUDSServer(udsAddr, dataMgr),
		udsAddr:    udsAddr,
		pluginAddr: "localhost:50052", // Default plugin server address
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
	module := task.Step.Call[0]
	name := task.Step.Call[1]

	fn, ok := GlobalRegistry.Get(module, name)
	if !ok {
		return "", fmt.Errorf("step implementation not found: %s:%s", module, name)
	}

	var input arrow.Record
	var err error

	// If a REMOTE ticket is provided, fetch data from the peer first
	if task.RemoteTicket != nil && task.RemoteTicket.RouteType == proto.RouteType_REMOTE {
		logger.L().Info("Fetching remote data",
			logger.String("peer", task.RemoteTicket.Address),
			logger.String("resID", task.RemoteTicket.ResourceId))

		localHandle, err := w.fetchRemoteData(ctx, task.RemoteTicket)
		if err != nil {
			return "", fmt.Errorf("failed to fetch remote data: %w", err)
		}
		task.InputHandle = localHandle
	}

	if task.InputHandle != "" {
		input, err = w.dataMgr.Get(task.InputHandle)
		if err != nil {
			return "", fmt.Errorf("failed to get input from shm: %w", err)
		}
		defer input.Release()
	}

	output, err := fn(ctx, input)
	if err != nil {
		return "", err
	}

	if output != nil {
		defer output.Release()
		handle := fmt.Sprintf("shm-%s-%d", task.ID, time.Now().UnixNano())
		if err := w.dataMgr.Put(handle, output); err != nil {
			return "", fmt.Errorf("failed to put output to shm: %w", err)
		}
		return handle, nil
	}

	return "", nil
}

// DoExchange implements the plugin server's exchange logic.
func (w *Worker) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	logger.L().Info("New plugin client connected via DoExchange")
	for {
		_, err := stream.Recv()
		if err != nil {
			return err
		}
		// Here we would send tasks to the plugin
	}
}
