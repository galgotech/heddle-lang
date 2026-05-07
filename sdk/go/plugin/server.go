package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"runtime/debug"
	"sync"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/galgotech/heddle-lang/pkg/runtime/data"
	"github.com/galgotech/heddle-lang/sdk/go/core"
	pb "github.com/galgotech/heddle-lang/sdk/go/proto"
)

// Server is the main Plugin server implementing the Arrow Flight interface.
type Server struct {
	flight.BaseFlightServer
	registry  *Registry
	resources sync.Map // map[string]interface{} (UUID -> Resource Instance)
	Namespace string
	Language  string
}

// NewServer creates a new Server with a given registry and namespace.
func NewServer(registry *Registry, namespace string) *Server {
	return &Server{
		registry:  registry,
		Namespace: namespace,
		Language:  "go",
	}
}

// handleExecutionError maps domain errors to protobuf status codes.
func handleExecutionError(err error) (pb.StatusCode, string) {
	if err == nil {
		return pb.StatusCode_SUCCESS, ""
	}

	var bErr *core.BusinessError
	if errors.As(err, &bErr) {
		return pb.StatusCode_BUSINESS_ERROR, bErr.Error()
	}

	return pb.StatusCode_FATAL_ERROR, err.Error()
}

// InitResource creates and stores a stateful resource.
func (s *Server) InitResource(ctx context.Context, req *pb.InitResourceRequest) (res *pb.InitResourceResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			stackTrace := string(debug.Stack())
			errMsg := fmt.Sprintf("Panic recovered: %v\n%s", r, stackTrace)
			res = &pb.InitResourceResponse{
				Status:       pb.StatusCode_FATAL_ERROR,
				ErrorMessage: errMsg,
			}
			err = nil // Do not return a gRPC error so the client can read the response payload
		}
	}()

	reg, ok := s.registry.GetResource(req.ResourceName)
	if !ok {
		return &pb.InitResourceResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: fmt.Sprintf("resource %q not found", req.ResourceName),
		}, nil
	}

	fnType := reg.Fn.Type()
	configType := fnType.In(1)

	// Create a new instance of the configuration struct
	var configVal reflect.Value
	if configType.Kind() == reflect.Ptr {
		configVal = reflect.New(configType.Elem())
	} else {
		configVal = reflect.New(configType)
	}

	// Unmarshal config JSON
	if req.ConfigJson != "" {
		if err := json.Unmarshal([]byte(req.ConfigJson), configVal.Interface()); err != nil {
			return &pb.InitResourceResponse{
				Status:       pb.StatusCode_FATAL_ERROR,
				ErrorMessage: fmt.Sprintf("failed to parse resource config: %v", err),
			}, nil
		}
	}

	if configType.Kind() != reflect.Ptr {
		configVal = configVal.Elem()
	}

	// Invoke the resource function
	args := []reflect.Value{reflect.ValueOf(ctx), configVal}
	results := reg.Fn.Call(args)

	errVal := results[1]
	if !errVal.IsNil() {
		err := errVal.Interface().(error)
		statusCode, errMsg := handleExecutionError(err)
		return &pb.InitResourceResponse{
			Status:       statusCode,
			ErrorMessage: errMsg,
		}, nil
	}

	resVal := results[0]

	// If resource implements core.Resource, call Start()
	if resVal.Type().Implements(reflect.TypeOf((*core.Resource)(nil)).Elem()) {
		if err := resVal.Interface().(core.Resource).Start(ctx); err != nil {
			statusCode, errMsg := handleExecutionError(err)
			return &pb.InitResourceResponse{
				Status:       statusCode,
				ErrorMessage: errMsg,
			}, nil
		}
	}

	// Generate ID and store
	resID := uuid.New().String()
	s.resources.Store(resID, resVal.Interface())

	return &pb.InitResourceResponse{
		Status:     pb.StatusCode_SUCCESS,
		ResourceId: resID,
	}, nil
}

// ExecuteStep runs a user-defined step function, injecting resources as needed.
func (s *Server) ExecuteStep(ctx context.Context, req *pb.ExecuteStepRequest) (res *pb.ExecuteStepResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			stackTrace := string(debug.Stack())
			errMsg := fmt.Sprintf("Panic recovered: %v\n%s", r, stackTrace)
			res = &pb.ExecuteStepResponse{
				Status:       pb.StatusCode_FATAL_ERROR,
				ErrorMessage: errMsg,
			}
			err = nil // Do not return a gRPC error so the client can read the response payload
		}
	}()

	reg, ok := s.registry.GetStep(req.StepName)
	if !ok {
		return &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: fmt.Sprintf("step %q not found", req.StepName),
		}, nil
	}

	fnType := reg.Fn.Type()
	configType := fnType.In(1)

	// Parse Configuration
	var configVal reflect.Value
	if configType.Kind() == reflect.Ptr {
		configVal = reflect.New(configType.Elem())
	} else {
		configVal = reflect.New(configType)
	}

	if req.ConfigJson != "" {
		if err := json.Unmarshal([]byte(req.ConfigJson), configVal.Interface()); err != nil {
			return &pb.ExecuteStepResponse{
				Status:       pb.StatusCode_FATAL_ERROR,
				ErrorMessage: fmt.Sprintf("failed to parse step config: %v", err),
			}, nil
		}
	}

	if configType.Kind() != reflect.Ptr {
		configVal = configVal.Elem()
	}

	// Set up arguments for step call
	args := []reflect.Value{reflect.ValueOf(ctx), configVal}

	// Inject Resource if requested
	if reg.ResourceName != "" {
		if req.ResourceId == "" {
			return &pb.ExecuteStepResponse{
				Status:       pb.StatusCode_FATAL_ERROR,
				ErrorMessage: fmt.Sprintf("step %q requires resource %q, but no resource ID was provided", req.StepName, reg.ResourceName),
			}, nil
		}

		resInstance, ok := s.resources.Load(req.ResourceId)
		if !ok {
			return &pb.ExecuteStepResponse{
				Status:       pb.StatusCode_FATAL_ERROR,
				ErrorMessage: fmt.Sprintf("resource instance %q not found", req.ResourceId),
			}, nil
		}

		// Optional: Verify resource type matches expected parameter
		args = append(args, reflect.ValueOf(resInstance))
	}

	// Create and append input table
	var inputTable core.Table
	var errInput error
	if req.GetInputHandle() != "" {
		if req.GetWorkerUdsPath() != "" {
			// Zero-copy path via Worker UDS
			ticket := &pb.FlightTicket{
				RouteType:  pb.RouteType_LOCAL,
				Address:    "unix://" + req.GetWorkerUdsPath(),
				ResourceId: req.GetInputHandle(),
			}
			inputTable, errInput = core.ResolveTicket(ctx, ticket)
		} else {
			// Fallback to direct file access
			inputTable, errInput = core.ReadTableFromHandle(req.GetInputHandle())
		}
	} else {
		inputTable, errInput = core.NewTableFromBytes(req.GetInputTable())
	}

	if errInput != nil {
		return &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: fmt.Sprintf("failed to prepare input table: %v", errInput),
		}, nil
	}
	args = append(args, reflect.ValueOf(inputTable))

	// Execute user function
	results := reg.Fn.Call(args)

	errVal := results[1]
	if !errVal.IsNil() {
		err := errVal.Interface().(error)
		statusCode, errMsg := handleExecutionError(err)
		return &pb.ExecuteStepResponse{
			Status:       statusCode,
			ErrorMessage: errMsg,
		}, nil
	}

	// Extract return table
	outTableVal := results[0]
	if outTableVal.IsNil() {
		return &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: "step function returned nil table",
		}, nil
	}
	outTable := outTableVal.Interface().(core.Table)

	// If an output handle is requested, write to it (zero-copy)
	if req.OutputHandle != "" {
		handlePath := req.OutputHandle
		if !core.IsAbsolutePath(handlePath) {
			handlePath = core.GetSharedMemoryPath(req.OutputHandle)
		}
		if err := outTable.WriteToHandle(handlePath); err != nil {
			return &pb.ExecuteStepResponse{
				Status:       pb.StatusCode_FATAL_ERROR,
				ErrorMessage: fmt.Sprintf("failed to write output table to handle: %v", err),
			}, nil
		}
		return &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_SUCCESS,
			OutputHandle: req.OutputHandle,
		}, nil
	}

	outputBytes, err := outTable.ToBytes()
	if err != nil {
		return &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: fmt.Sprintf("failed to serialize output table: %v", err),
		}, nil
	}

	return &pb.ExecuteStepResponse{
		Status:      pb.StatusCode_SUCCESS,
		OutputTable: outputBytes,
	}, nil
}

// Describe returns metadata about all registered steps and resources.
func (s *Server) Describe(ctx context.Context, _ *emptypb.Empty) (*pb.DescribeResponse, error) {
	var steps []*pb.StepMetadata
	for _, reg := range s.registry.steps {
		steps = append(steps, &pb.StepMetadata{
			Name:             reg.Name,
			ConfigJsonSchema: reg.ConfigSchema,
			RequiresResource: reg.ResourceName != "",
			ResourceName:     reg.ResourceName,
		})
	}

	var resources []*pb.ResourceMetadata
	for _, reg := range s.registry.resources {
		resources = append(resources, &pb.ResourceMetadata{
			Name:             reg.Name,
			ConfigJsonSchema: reg.ConfigSchema,
		})
	}

	return &pb.DescribeResponse{
		Namespace: s.Namespace,
		Steps:     steps,
		Resources: resources,
	}, nil
}

// DoAction handles custom actions like Describe and InitResource.
func (s *Server) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	ctx := stream.Context()
	switch action.Type {
	case "handshake":
		var req pb.HandshakeRequest
		if err := proto.Unmarshal(action.Body, &req); err != nil {
			return err
		}
		// Verify namespace if requested, or just acknowledge
		res := &pb.HandshakeResponse{
			Status: pb.StatusCode_SUCCESS,
		}
		if req.Namespace != "" && req.Namespace != s.Namespace {
			res.Status = pb.StatusCode_FATAL_ERROR
			res.ErrorMessage = fmt.Sprintf("namespace mismatch: expected %s, got %s", s.Namespace, req.Namespace)
		}
		body, err := proto.Marshal(res)
		if err != nil {
			return err
		}
		return stream.Send(&flight.Result{Body: body})

	case "describe":
		res, err := s.Describe(ctx, &emptypb.Empty{})
		if err != nil {
			return err
		}
		body, err := proto.Marshal(res)
		if err != nil {
			return err
		}
		return stream.Send(&flight.Result{Body: body})

	case "init_resource":
		var req pb.InitResourceRequest
		if err := proto.Unmarshal(action.Body, &req); err != nil {
			return err
		}
		res, err := s.InitResource(ctx, &req)
		if err != nil {
			return err
		}
		body, err := proto.Marshal(res)
		if err != nil {
			return err
		}
		return stream.Send(&flight.Result{Body: body})

	default:
		return fmt.Errorf("unknown action type: %s", action.Type)
	}
}

// DoExchange handles ExecuteStep via bidirectional stream.
func (s *Server) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	// 1. Read first message for ExecuteStepRequest metadata
	msg, err := stream.Recv()
	if err != nil {
		return err
	}

	var req pb.ExecuteStepRequest
	if err := proto.Unmarshal(msg.AppMetadata, &req); err != nil {
		return fmt.Errorf("failed to unmarshal ExecuteStepRequest from AppMetadata: %w", err)
	}

	ctx := stream.Context()
	var inputTable core.Table

	// 2. Fetch input table (either via handle or streaming)
	if req.InputHandle != "" {
		if req.WorkerUdsPath != "" {
			ticket := &pb.FlightTicket{
				RouteType:  pb.RouteType_LOCAL,
				Address:    "unix://" + req.WorkerUdsPath,
				ResourceId: req.InputHandle,
			}
			inputTable, err = core.ResolveTicket(ctx, ticket)
		} else {
			inputTable, err = core.ReadTableFromHandle(req.InputHandle)
		}
		if err != nil {
			return fmt.Errorf("failed to resolve input handle: %w", err)
		}
	} else if len(req.InputTable) > 0 {
		// Read input table from stream (as RecordBatches)
		reader, err := flight.NewRecordReader(stream, ipc.WithAllocator(memory.DefaultAllocator))
		if err != nil {
			return fmt.Errorf("failed to create record reader: %w", err)
		}
		defer reader.Release()

		if !reader.Next() {
			return fmt.Errorf("no input data received")
		}
		rec := reader.Record()
		rec.Retain()
		inputTable = core.NewTableFromRecord(rec)
	} else {
		// No input provided
		inputTable, _ = core.NewTableFromBytes(nil)
	}
	defer inputTable.Release()

	// 3. Execute step
	res, err := s.executeStepWithTable(ctx, &req, inputTable)
	if err != nil {
		return err
	}

	// 4. Send back response metadata
	resMeta, _ := proto.Marshal(res)
	if err := stream.Send(&flight.FlightData{AppMetadata: resMeta}); err != nil {
		return err
	}

	// 5. Send back output table if it's not handle-based
	if res.OutputHandle == "" && res.OutputTable != nil {
		outTable, err := core.NewTableFromBytes(res.OutputTable)
		if err != nil {
			return fmt.Errorf("failed to prepare output table: %w", err)
		}
		defer outTable.Release()

		if outTable.Native() != nil {
			writer := flight.NewRecordWriter(stream, ipc.WithAllocator(memory.DefaultAllocator), ipc.WithSchema(outTable.Native().Schema()))
			if err := writer.Write(outTable.Native()); err != nil {
				return fmt.Errorf("failed to write output record: %w", err)
			}
			return writer.Close()
		}
	}

	return nil
}

// executeStepWithTable is a helper that takes an already parsed Table.
func (s *Server) executeStepWithTable(ctx context.Context, req *pb.ExecuteStepRequest, inputTable core.Table) (*pb.ExecuteStepResponse, error) {
	reg, ok := s.registry.GetStep(req.StepName)
	if !ok {
		return &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: fmt.Sprintf("step %q not found", req.StepName),
		}, nil
	}

	fnType := reg.Fn.Type()
	configType := fnType.In(1)

	// Parse Configuration
	var configVal reflect.Value
	if configType.Kind() == reflect.Ptr {
		configVal = reflect.New(configType.Elem())
	} else {
		configVal = reflect.New(configType)
	}

	if req.ConfigJson != "" {
		if err := json.Unmarshal([]byte(req.ConfigJson), configVal.Interface()); err != nil {
			return &pb.ExecuteStepResponse{
				Status:       pb.StatusCode_FATAL_ERROR,
				ErrorMessage: fmt.Sprintf("failed to parse step config: %v", err),
			}, nil
		}
	}

	if configType.Kind() != reflect.Ptr {
		configVal = configVal.Elem()
	}

	// Set up arguments for step call
	args := []reflect.Value{reflect.ValueOf(ctx), configVal}

	// Inject Resource if requested
	if reg.ResourceName != "" {
		if req.ResourceId == "" {
			return &pb.ExecuteStepResponse{
				Status:       pb.StatusCode_FATAL_ERROR,
				ErrorMessage: fmt.Sprintf("step %q requires resource %q, but no resource ID was provided", req.StepName, reg.ResourceName),
			}, nil
		}

		resInstance, ok := s.resources.Load(req.ResourceId)
		if !ok {
			return &pb.ExecuteStepResponse{
				Status:       pb.StatusCode_FATAL_ERROR,
				ErrorMessage: fmt.Sprintf("resource instance %q not found", req.ResourceId),
			}, nil
		}

		args = append(args, reflect.ValueOf(resInstance))
	}

	args = append(args, reflect.ValueOf(inputTable))

	// Execute user function
	results := reg.Fn.Call(args)

	errVal := results[1]
	if !errVal.IsNil() {
		err := errVal.Interface().(error)
		statusCode, errMsg := handleExecutionError(err)
		return &pb.ExecuteStepResponse{
			Status:       statusCode,
			ErrorMessage: errMsg,
		}, nil
	}

	// Extract return table
	outTableVal := results[0]
	if outTableVal.IsNil() {
		return &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: "step function returned nil table",
		}, nil
	}
	outTable := outTableVal.Interface().(core.Table)

	// If an output handle is requested, write to it (zero-copy)
	if req.OutputHandle != "" {
		handlePath := req.OutputHandle
		if !core.IsAbsolutePath(handlePath) {
			handlePath = core.GetSharedMemoryPath(req.OutputHandle)
		}
		if err := outTable.WriteToHandle(handlePath); err != nil {
			return &pb.ExecuteStepResponse{
				Status:       pb.StatusCode_FATAL_ERROR,
				ErrorMessage: fmt.Sprintf("failed to write output table to handle: %v", err),
			}, nil
		}
		return &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_SUCCESS,
			OutputHandle: req.OutputHandle,
		}, nil
	}

	outputBytes, err := outTable.ToBytes()
	if err != nil {
		return &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: fmt.Sprintf("failed to serialize output table: %v", err),
		}, nil
	}

	return &pb.ExecuteStepResponse{
		Status:      pb.StatusCode_SUCCESS,
		OutputTable: outputBytes,
	}, nil
}

// Plugin wraps the registry and server to provide the user-facing API.
type Plugin struct {
	registry *Registry
	server   *Server
}

// New creates a new SDK Plugin instance with the specified namespace.
func New(namespace string) *Plugin {
	registry := NewRegistry()
	return &Plugin{
		registry: registry,
		server:   NewServer(registry, namespace),
	}
}

// RegisterResource is a proxy to Registry.RegisterResource.
func (p *Plugin) RegisterResource(name string, fn interface{}) {
	p.registry.RegisterResource(name, fn)
}

// RegisterStep is a proxy to Registry.RegisterStep.
func (p *Plugin) RegisterStep(name string, fn interface{}, opts ...StepOption) {
	p.registry.RegisterStep(name, fn, opts...)
}

// Serve starts the gRPC server on the address specified by the PORT environment variable, defaulting to 50051.
func (p *Plugin) Serve() error {
	port := os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}
	addr := ":" + port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	return p.ServeListener(lis)
}

// ServeListener starts the Arrow Flight server and UDS handler on a specific listener.
func (p *Plugin) ServeListener(lis net.Listener) error {
	// If it's a unix socket, we use a multiplexing listener to handle both gRPC and raw UDS.
	if _, ok := lis.(*net.UnixListener); ok {
		mux := &multiplexListener{Listener: lis, server: p.server}
		grpcServer := grpc.NewServer()
		flight.RegisterFlightServiceServer(grpcServer, p.server)
		return grpcServer.Serve(mux)
	}

	grpcServer := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcServer, p.server)
	return grpcServer.Serve(lis)
}

type multiplexListener struct {
	net.Listener
	server *Server
}

func (l *multiplexListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}

		unixConn := conn.(*net.UnixConn)
		// We use a small peek to see if it's gRPC (HTTP/2) or our raw UDS protocol.
		// gRPC preface starts with "PRI "
		buf := make([]byte, 4)
		oob := make([]byte, 64)
		n, oobn, _, _, err := unixConn.ReadMsgUnix(buf, oob)
		if err != nil {
			unixConn.Close()
			continue
		}

		if string(buf[:n]) == "PRI " {
			// It's gRPC. We need to "un-read" these bytes.
			return &peekConn{UnixConn: unixConn, peeked: buf[:n]}, nil
		}

		// It's our raw UDS protocol. We handle it in a goroutine and keep accepting.
		go l.server.HandleUDSWithInitialData(unixConn, buf[:n], oob[:oobn])
	}
}

type peekConn struct {
	*net.UnixConn
	peeked []byte
}

func (c *peekConn) Read(b []byte) (int, error) {
	if len(c.peeked) > 0 {
		n := copy(b, c.peeked)
		c.peeked = c.peeked[n:]
		return n, nil
	}
	return c.UnixConn.Read(b)
}

// HandleUDSWithInitialData is like HandleUDSConnection but with already read data and OOB.
func (s *Server) HandleUDSWithInitialData(conn *net.UnixConn, initialData []byte, initialOOB []byte) {
	defer conn.Close()

	// Receive the rest of metadata (if any)
	buf := make([]byte, 4096)
	n, _, _, _, _ := conn.ReadMsgUnix(buf, nil)

	// Combine initial data with the rest of metadata
	meta := append(initialData, buf[:n]...)

	// Parse OOB (FDs)
	msgs, err := unix.ParseSocketControlMessage(initialOOB)
	if err != nil || len(msgs) == 0 {
		return
	}
	fds, err := unix.ParseUnixRights(&msgs[0])
	if err != nil || len(fds) == 0 {
		return
	}
	fd := fds[0]
	defer os.NewFile(uintptr(fd), "shm").Close()

	var req pb.ExecuteStepRequest
	if err := proto.Unmarshal(meta, &req); err != nil {
		return
	}

	// Create Table from FD
	inputTable, err := core.ReadTableFromFD(fd)
	if err != nil {
		resp := &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: fmt.Sprintf("failed to read table from FD: %v", err),
		}
		s.respondUDS(conn, resp)
		return
	}
	defer inputTable.Release()

	// Execute step
	res, err := s.executeStepWithTable(context.Background(), &req, inputTable)
	if err != nil {
		res = &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: err.Error(),
		}
	}

	s.respondUDS(conn, res)
}

func (s *Server) AcceptUDS(lis net.Listener) {
	// No longer used, replaced by multiplexListener
}

// HandleUDSConnection handles a raw UDS connection for proactive FD passing.
func (s *Server) HandleUDSConnection(conn net.Conn) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		conn.Close()
		return
	}

	// We try to receive FD and metadata. If it fails or doesn't look like our protocol,
	// we just close it (since we are not multiplexing perfectly with gRPC yet).
	// In a real scenario, the worker knows which protocol to use.
	fd, meta, err := data.RecvFDWithMetadata(unixConn)
	if err != nil {
		unixConn.Close()
		return
	}
	defer os.NewFile(uintptr(fd), "shm").Close()

	var req pb.ExecuteStepRequest
	if err := proto.Unmarshal(meta, &req); err != nil {
		unixConn.Close()
		return
	}

	// Create Table from FD
	inputTable, err := core.ReadTableFromFD(fd)
	if err != nil {
		resp := &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: fmt.Sprintf("failed to read table from FD: %v", err),
		}
		s.respondUDS(unixConn, resp)
		return
	}
	defer inputTable.Release()

	// Execute step
	res, err := s.executeStepWithTable(context.Background(), &req, inputTable)
	if err != nil {
		res = &pb.ExecuteStepResponse{
			Status:       pb.StatusCode_FATAL_ERROR,
			ErrorMessage: err.Error(),
		}
	}

	s.respondUDS(unixConn, res)
}

func (s *Server) respondUDS(conn *net.UnixConn, resp *pb.ExecuteStepResponse) {
	defer conn.Close()
	data, _ := proto.Marshal(resp)
	_, _ = conn.Write(data)
}

// RegisterStep is a generic helper to register a step function with strict signature.
func RegisterStep[C any](p *Plugin, name string, fn StepFunc[C]) {
	p.RegisterStep(name, fn)
}

// RegisterStepWithResource is a generic helper to register a step function that requires a resource.
func RegisterStepWithResource[C any, R any](p *Plugin, name string, resourceName string, fn ResourceStepFunc[C, R]) {
	p.RegisterStep(name, fn, WithResource(resourceName))
}

// RegisterResource is a generic helper to register a resource function with strict signature.
func RegisterResource[C any, R any](p *Plugin, name string, fn ResourceFunc[C, R]) {
	p.RegisterResource(name, fn)
}
