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
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/galgotech/heddle-lang/sdk/go/core"
	pb "github.com/galgotech/heddle-lang/sdk/go/proto"
)

// Server is the main Plugin server implementing the Arrow Flight interface.
type Server struct {
	flight.BaseFlightServer
	registry  *Registry
	resources sync.Map // map[string]interface{} (UUID -> Resource Instance)
}

// NewServer creates a new Server with a given registry.
func NewServer(registry *Registry) *Server {
	return &Server{
		registry: registry,
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
	var inputTable *core.Table
	var errInput error
	if req.GetInputHandle() != "" {
		inputTable, errInput = core.ReadTableFromHandle(req.GetInputHandle())
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
	outTable := outTableVal.Interface().(*core.Table)

	// If an output handle is requested, write to it (zero-copy)
	if req.OutputHandle != "" {
		if err := outTable.WriteToHandle(req.OutputHandle); err != nil {
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
		Steps:     steps,
		Resources: resources,
	}, nil
}

// DoAction handles custom actions like Describe and InitResource.
func (s *Server) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	ctx := stream.Context()
	switch action.Type {
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

	// 2. Read input table from stream (as RecordBatches)
	reader, err := flight.NewRecordReader(stream, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	// Accumulate records if needed, or process them as they come.
	// For simplicity, we'll convert to a core.Table (which currently holds arrow.Record).
	// Actually, core.Table might need to be updated to handle streaming.
	// But let's assume we read the first record for now, or concatenate.

	if !reader.Next() {
		return fmt.Errorf("no input data received")
	}
	rec := reader.Record()
	rec.Retain()
	defer rec.Release()

	inputTable := core.NewTableFromRecord(rec)

	// 3. Execute step
	// We need to adapt the execute logic.
	// I'll call a modified version of ExecuteStep or just inline the logic.

	res, err := s.executeStepWithTable(context.Background(), &req, inputTable)
	if err != nil {
		return err
	}

	// 4. Send back output table as RecordBatches
	if res.OutputTable != nil || res.OutputHandle != "" {
		// If we have an output table in memory, stream it back.
		var outTable *core.Table
		if res.OutputHandle != "" {
			outTable, err = core.ReadTableFromHandle(res.OutputHandle)
		} else {
			outTable, err = core.NewTableFromBytes(res.OutputTable)
		}

		if err != nil {
			return fmt.Errorf("failed to prepare output table: %w", err)
		}
		if outTable.Record == nil {
			return nil // Nothing to send
		}

		writer := flight.NewRecordWriter(stream, ipc.WithAllocator(memory.DefaultAllocator), ipc.WithSchema(outTable.Record.Schema()))
		if err := writer.Write(outTable.Record); err != nil {
			return fmt.Errorf("failed to write output record: %w", err)
		}
		return writer.Close()
	}

	return nil
}

// executeStepWithTable is a helper that takes an already parsed Table.
func (s *Server) executeStepWithTable(ctx context.Context, req *pb.ExecuteStepRequest, inputTable *core.Table) (*pb.ExecuteStepResponse, error) {
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
	outTable := outTableVal.Interface().(*core.Table)

	// If an output handle is requested, write to it (zero-copy)
	if req.OutputHandle != "" {
		if err := outTable.WriteToHandle(req.OutputHandle); err != nil {
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

// New creates a new SDK Plugin instance.
func New() *Plugin {
	registry := NewRegistry()
	return &Plugin{
		registry: registry,
		server:   NewServer(registry),
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

// ServeListener starts the Arrow Flight server on a specific listener.
func (p *Plugin) ServeListener(lis net.Listener) error {
	grpcServer := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcServer, p.server)
	return grpcServer.Serve(lis)
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
