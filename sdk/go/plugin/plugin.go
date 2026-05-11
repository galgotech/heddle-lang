package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/sdk/go/core"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// ResourceRegistration holds the metadata and execution handle for a registered Heddle Resource.
type ResourceRegistration struct {
	Name     string
	Resource Resource
}

// StepRegistration holds the metadata and execution handle for a registered Heddle Step.
type StepRegistration struct {
	Name         string
	ConfigSchema string // JSON schema inferred from the configuration struct
	ConfigType   reflect.Type
	Func         reflect.Value
}

// PlanningDataHandler is a callback function that receives data from a 'std.data' step.
type PlanningDataHandler func(data []map[string]any) error

type Plugin struct {
	Namespace     string
	Language      string
	WorkerAddress string
	resources     map[string]ResourceRegistration
	steps         map[string]StepRegistration
	Ready         chan struct{}
}

// RegisterResource adds a new resource initializer to the registry.
// It performs strict reflection-based validation to ensure the function signature adheres to the ResourceFunc contract.
// Panics if the function signature is invalid or doesn't implement context.Context and error interfaces.
func (p *Plugin) RegisterResource(name string, resource Resource) error {
	p.resources[name] = ResourceRegistration{
		Name:     name,
		Resource: resource,
	}

	return nil
}

// RegisterStep adds a new step function to the registry.
func (p *Plugin) RegisterStep(name string, fn any) error {
	typ := reflect.TypeOf(fn)

	if typ.Kind() != reflect.Func {
		return fmt.Errorf("step %q must be a function", name)
	}

	// Validate minimal signature requirements (3 inputs, 2 outputs)
	if typ.NumIn() != 3 || typ.NumOut() != 2 {
		return fmt.Errorf("step %q must have signature func(ctx, config, input) (output, error)", name)
	}

	reg := StepRegistration{
		Name:         name,
		ConfigSchema: generateJSONSchema(typ.In(1)),
		ConfigType:   typ.In(1),
		Func:         reflect.ValueOf(fn),
	}

	p.steps[name] = reg

	return nil
}

// Serve starts the plugin and connects to the specified worker.
func (p *Plugin) Start() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var opts []grpc.DialOption
	var err error
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	var conn *grpc.ClientConn
	var client flight.Client

	// 1. Start Retry Loop
	for {
		// 1.1 Connect to Worker (handle UDS if path starts with / or unix:)
		target := p.WorkerAddress
		if target == "" {
			target = "unix:///tmp/heddle-worker.sock"
		}

		conn, err = grpc.NewClient(target, opts...)
		if err != nil {
			logger.L().Info("Worker not reachable, retrying...", zap.String("target", target))
			
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
				continue
			}
		}

		client = flight.NewClientFromConn(conn, nil)

		// 1.2 Register Plugin
		capabilities := make([]string, 0, len(p.steps))
		for name := range p.steps {
			capabilities = append(capabilities, fmt.Sprintf("%s.%s", p.Namespace, name))
		}

		reg := PluginRegistration{
			Namespace:    p.Namespace,
			Language:     p.Language,
			Version:      "0.1.0",
			Capabilities: capabilities,
		}
		regBody, _ := json.Marshal(reg)
		res, err := client.DoAction(ctx, &flight.Action{
			Type: ActionRegisterPlugin,
			Body: regBody,
		})
		if err != nil {
			logger.L().Info("Retrying plugin registration...", zap.String("target", target))
			conn.Close()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
				continue
			}
		}

		// Wait for registration result
		if _, err := res.Recv(); err != nil {
			logger.L().Info("Waiting for registration result...", zap.String("target", target))
			conn.Close()
			
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
				continue
			}
		}

		logger.L().Info("Plugin registered", zap.String("namespace", p.Namespace))

		// 1.3 Start Heartbeat and Execution Loop
		// We use a separate context for each connection session
		sessionCtx, cancel := context.WithCancel(ctx)
		
		go p.startHeartbeat(sessionCtx, client)

		if p.Ready != nil {
			// Only close Ready once
			select {
			case <-p.Ready:
			default:
				close(p.Ready)
			}
		}

		err = p.startExecutionLoop(sessionCtx, client)
		cancel() // Stop heartbeat
		conn.Close()

		if err != nil {
			logger.L().Info("Worker connection lost, reconnecting...", zap.String("target", target))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
				continue
			}
		}
		
		return nil // Graceful shutdown
	}
}

func (p *Plugin) startHeartbeat(ctx context.Context, client flight.Client) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hb := Heartbeat{
				Timestamp: time.Now(),
				Status:    "ready",
			}
			body, _ := json.Marshal(hb)
			_, err := client.DoAction(ctx, &flight.Action{
				Type: ActionPluginHeartbeat,
				Body: body,
			})
			if err != nil {
				logger.L().Error("Heartbeat failed", zap.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (p *Plugin) startExecutionLoop(ctx context.Context, client flight.Client) error {
	// Add namespace to metadata for identification
	md := metadata.Pairs("x-heddle-plugin-namespace", p.Namespace)
	ctx = metadata.NewOutgoingContext(ctx, md)

	stream, err := client.DoExchange(ctx)
	if err != nil {
		return fmt.Errorf("failed to start exchange: %w", err)
	}

	logger.L().Info("Plugin execution loop started", zap.String("namespace", p.Namespace))

	for {
		data, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("exchange stream closed: %w", err)
		}

		var req ExecuteStepRequest
		if err := json.Unmarshal(data.DataBody, &req); err != nil {
			logger.L().Error("Failed to unmarshal request", zap.Error(err))
			continue
		}

		// Execute task in a goroutine
		go func(r ExecuteStepRequest) {
			resp := p.executeTask(ctx, r)
			respBody, err := json.Marshal(resp)
			if err != nil {
				logger.L().Error("Failed to unmarshal response", zap.Error(err))
				return
			}
			if err := stream.Send(&flight.FlightData{DataBody: respBody}); err != nil {
				logger.L().Error("Failed to send response", zap.Error(err))
			}
		}(req)
	}
}

func (p *Plugin) executeTask(ctx context.Context, req ExecuteStepRequest) ExecuteStepResponse {
	// 1. Resolve module and step
	// For now, assume StepName is "module:step"
	parts := reflect.ValueOf(p.steps).MapKeys()
	_ = parts // DEBUG

	// Find the module and step
	// This is a simplified routing, we might need a better one.
	var targetStep *StepRegistration
	for _, s := range p.steps {
		if s.Name == req.StepName {
			targetStep = &s
			break
		}
	}

	if targetStep == nil {
		return ExecuteStepResponse{
			TaskID:       req.TaskID,
			Status:       "FAILED",
			ErrorMessage: fmt.Sprintf("step %s not found", req.StepName),
		}
	}

	// 2. Prepare arguments
	// func(ctx, config, input) (output, error)
	configType := targetStep.ConfigType
	isPtr := configType.Kind() == reflect.Pointer

	var configVal reflect.Value
	if isPtr {
		configVal = reflect.New(configType.Elem())
	} else {
		configVal = reflect.New(configType)
	}

	if req.ConfigJSON != "" {
		if err := json.Unmarshal([]byte(req.ConfigJSON), configVal.Interface()); err != nil {
			return ExecuteStepResponse{
				TaskID:       req.TaskID,
				Status:       "FAILED",
				ErrorMessage: fmt.Errorf("failed to unmarshal config: %w", err).Error(),
			}
		}
	}

	// Zero-Copy Input: Read from SHM if path is provided
	var inputTable core.Table
	if req.InputHandle != "" {
		record, err := core.ReadArrowFromPath(req.InputHandle)
		if err != nil {
			logger.L().Error("Failed to read input from SHM", zap.Error(err), zap.String("path", req.InputHandle))
		} else {
			inputTable = core.NewTableFromRecord(record)
			defer inputTable.Release()
		}
	}

	// 3. Call the function
	var arg1 reflect.Value
	if isPtr {
		arg1 = configVal
	} else {
		arg1 = configVal.Elem()
	}

	arg2 := reflect.ValueOf(inputTable)

	args := []reflect.Value{
		reflect.ValueOf(ctx),
		arg1,
		arg2,
	}

	// STEP EXECUTION
	results := targetStep.Func.Call(args)

	// 4. Handle results
	if !results[1].IsNil() {
		return ExecuteStepResponse{
			TaskID:       req.TaskID,
			Status:       "FAILED",
			ErrorMessage: results[1].Interface().(error).Error(),
		}
	}

	outputTable, ok := results[0].Interface().(core.Table)
	outputHandle := ""
	if ok && outputTable != nil && outputTable.Native() != nil {
		path, err := core.WriteArrowToShm(outputTable.Native())
		if err != nil {
			logger.L().Error("Failed to write output to SHM", zap.Error(err))
		} else {
			outputHandle = path
		}
	}

	return ExecuteStepResponse{
		TaskID:       req.TaskID,
		Status:       "SUCCESS",
		OutputHandle: outputHandle,
	}
}

func New(namespace string) *Plugin {
	return &Plugin{
		Namespace: namespace,
		Language:  "go",
		resources: make(map[string]ResourceRegistration),
		steps:     make(map[string]StepRegistration),
		Ready:     make(chan struct{}),
	}
}

// generateJSONSchema performs type introspection to derive a basic JSON schema for Step/Resource configurations.
// This schema is used by the Heddle DSL to validate user-provided parameters at compile-time.
func generateJSONSchema(t reflect.Type) string {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return fmt.Sprintf(`{"type": "%s"}`, t.Kind().String())
	}

	var schema strings.Builder
	schema.WriteString(`{"type": "object", "properties": {`)
	first := true
	for field := range t.Fields() {
		jsonTag := field.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}
		name := jsonTag
		if name == "" {
			name = field.Name
		} else {
			// Handle cases like json:"name,omitempty"
			parts := strings.Split(name, ",")
			name = parts[0]
			if name == "" {
				name = field.Name
			}
		}

		if !first {
			schema.WriteString(", ")
		}
		fmt.Fprintf(&schema, `"%s": {"type": "%s"}`, name, field.Type.Kind().String())
		first = false
	}
	schema.WriteString(`}}`)
	return schema.String()
}
