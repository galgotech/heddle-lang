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

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/galgotech/heddle-lang/sdk/go/core"
	pb "github.com/galgotech/heddle-lang/sdk/go/proto"
)

// Server is the main Plugin server implementing the gRPC PluginService.
type Server struct {
	pb.UnimplementedPluginServiceServer
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

// ServeListener starts the gRPC server on a specific listener.
func (p *Plugin) ServeListener(lis net.Listener) error {
	grpcServer := grpc.NewServer()
	pb.RegisterPluginServiceServer(grpcServer, p.server)
	return grpcServer.Serve(lis)
}
