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

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/pkg/schema"
	"github.com/galgotech/heddle-lang/sdk/go/core"
)

// ResourceRegistration maintains metadata and the execution handle for a Heddle Resource.
// It allows the plugin to expose custom infrastructure or stateful components to the Heddle DSL.
type ResourceRegistration struct {
	Name     string
	Resource Resource
}

// StepRegistration stores the execution contract for a Heddle Step.
// It captures the function signature, inferred JSON schemas for configuration,
// and the mapping between Arrow schemas and Go struct types.
type StepRegistration struct {
	Name         string
	ConfigSchema string // JSON schema inferred from the configuration struct for DSL-side validation
	ConfigType   reflect.Type
	InputType    reflect.Type
	OutputType   reflect.Type
	Func         reflect.Value
	InputSchema  *schema.FrameSchema
	OutputSchema *schema.FrameSchema
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

// RegisterResource adds a new resource initializer to the plugin's internal registry.
// These resources can be referenced in .he files to manage external state or connections.
func (p *Plugin) RegisterResource(name string, resource Resource) error {
	p.resources[name] = ResourceRegistration{
		Name:     name,
		Resource: resource,
	}

	return nil
}

// RegisterStep registers a Go function as a Heddle Step.
// It performs reflection-based validation of the function signature: func(ctx, config, input) (output, error).
// It also extracts Heddle-compatible schemas from the input and output types for compile-time DSL validation.
func (p *Plugin) RegisterStep(name string, fn any) error {
	typ := reflect.TypeOf(fn)

	if typ.Kind() != reflect.Func {
		return fmt.Errorf("step %q must be a function", name)
	}

	// Ensure the function signature matches the expected contract:
	// func(context.Context, TConfig, TInput) (TOutput, error)
	if typ.NumIn() != 3 || typ.NumOut() != 2 {
		return fmt.Errorf("step %q must have signature func(ctx, config, input) (output, error)", name)
	}

	inputSchema, err := ExtractSchema(typ.In(2))
	if err != nil {
		return fmt.Errorf("step %q input: %w", name, err)
	}

	outputSchema, err := ExtractSchema(typ.Out(0))
	if err != nil {
		return fmt.Errorf("step %q output: %w", name, err)
	}

	reg := StepRegistration{
		Name:         name,
		ConfigSchema: generateJSONSchema(typ.In(1)),
		ConfigType:   typ.In(1),
		InputType:    typ.In(2),
		OutputType:   typ.Out(0),
		Func:         reflect.ValueOf(fn),
		InputSchema:  inputSchema,
		OutputSchema: outputSchema,
	}

	p.steps[name] = reg

	return nil
}

// Start initializes the plugin's lifecycle, establishing a resilient connection to the Worker.
// It manages registration, heartbeats, and the bidirectional execution stream.
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

		// Establish the gRPC connection to the Worker.
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
		schemas := make(map[string]schema.StepSchemas)
		for name, step := range p.steps {
			capName := fmt.Sprintf("%s.%s", p.Namespace, name)
			capabilities = append(capabilities, capName)
			schemas[capName] = schema.StepSchemas{
				Input:  step.InputSchema,
				Output: step.OutputSchema,
			}
		}

		reg := PluginRegistration{
			Namespace:    p.Namespace,
			Language:     p.Language,
			Version:      "0.1.0",
			Capabilities: capabilities,
			Schemas:      schemas,
		}
		regBody, _ := json.Marshal(reg)
		// Submit registration via Arrow Flight DoAction.
		// This notifies the Worker of the plugin's namespace and step capabilities.
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

		// Block until the Worker acknowledges registration.
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

// startHeartbeat periodically signals the plugin's health and availability to the Worker.
func (p *Plugin) startHeartbeat(ctx context.Context, client flight.Client) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hb := Heartbeat{
				Namespace: p.Namespace,
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

// startExecutionLoop opens a bidirectional Arrow Flight exchange for processing step tasks.
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

// executeTask handles the end-to-end execution of a single Heddle Step.
// It performs Zero-Copy data loading from SHM, reflection-based binding to Go structs,
// function invocation, and result serialization back to SHM.
func (p *Plugin) executeTask(ctx context.Context, req ExecuteStepRequest) ExecuteStepResponse {
	// 1. Resolve the requested step in this plugin's namespace.
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

	// 2. Hydrate the step configuration from the provided JSON.
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

	// 3. Prepare the Input Frame using Zero-Copy SHM access.
	// If an InputHandle is provided, the Arrow data is mapped directly from shared memory.
	var arrowTable arrow.Table
	if req.InputHandle != "" {
		record, err := core.ReadArrowFromPath(req.InputHandle)
		if err != nil {
			logger.L().Error("Failed to read input from SHM", zap.Error(err), zap.String("path", req.InputHandle))
		} else {
			arrowTable = array.NewTableFromRecords(record.Schema(), []arrow.Record{record})
			defer arrowTable.Release()
		}
	}

	inputVal := reflect.New(targetStep.InputType).Elem()
	if hasHeddleFrame(targetStep.InputType) {
		if arrowTable != nil {
			if err := bindFrameValue(inputVal, arrowTable); err != nil {
				return ExecuteStepResponse{
					TaskID:       req.TaskID,
					Status:       "FAILED",
					ErrorMessage: fmt.Sprintf("failed to bind input frame: %v", err),
				}
			}
		}
	}

	var arg1 reflect.Value
	if isPtr {
		arg1 = configVal
	} else {
		arg1 = configVal.Elem()
	}

	args := []reflect.Value{
		reflect.ValueOf(ctx),
		arg1,
		inputVal,
	}

	// 4. Call the function
	results := targetStep.Func.Call(args)

	// 5. Handle output results and commit data to SHM.
	if !results[1].IsNil() {
		return ExecuteStepResponse{
			TaskID:       req.TaskID,
			Status:       "FAILED",
			ErrorMessage: results[1].Interface().(error).Error(),
		}
	}

	outputVal := results[0]
	outputHandle := ""
	dirtyHandle := ""

	// Check if the output is a VoidFrame (explicitly no-data).
	if targetStep.OutputType == reflect.TypeFor[VoidFrame]() {
		return ExecuteStepResponse{
			TaskID:       req.TaskID,
			Status:       "SUCCESS",
			OutputHandle: "",
		}
	}

	// If the output contains a HeddleFrame, persist the Arrow data to Shared Memory.
	if hasHeddleFrame(targetStep.OutputType) {
		frameField := findHeddleFrameField(outputVal)
		if frameField.IsValid() {
			hf := frameField.Addr().Interface().(*HeddleFrame)
			if hf.native != nil {
				// 5.1 Write the Arrow RecordBatch to SHM.
				reader := array.NewTableReader(hf.native, hf.native.NumRows())
				if reader.Next() {
					rec := reader.Record()
					path, err := core.WriteArrowToShm(rec)
					if err != nil {
						logger.L().Error("Failed to write output to SHM", zap.Error(err))
					} else {
						outputHandle = path

						// 5.2 Write the Dirty Bitmap to SHM.
						// This allows downstream steps to identify which rows were modified.
						hasDirty := false
						for _, d := range hf.dirtyBitmap() {
							if d != 0 {
								hasDirty = true
								break
							}
						}

						if hasDirty {
							dp, err := locality.WriteDirtyToShm(hf.dirtyBitmap())
							if err != nil {
								logger.L().Error("Failed to write dirty bits to SHM", zap.Error(err))
							} else {
								dirtyHandle = dp
							}
						}
					}
				}
				reader.Release()
			}
		}
	}

	return ExecuteStepResponse{
		TaskID:       req.TaskID,
		Status:       "SUCCESS",
		OutputHandle: outputHandle,
		DirtyHandle:  dirtyHandle,
	}
}

// New creates a new Heddle Plugin instance within the specified namespace.
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
// The Heddle DSL compiler uses this schema to validate user-provided parameters during the compilation phase.
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

// hasHeddleFrame checks if a given struct type contains a HeddleFrame field.
func hasHeddleFrame(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Type == reflect.TypeOf(HeddleFrame{}) {
			return true
		}
	}
	return false
}

// findHeddleFrameField retrieves the reflect.Value of the HeddleFrame field within a struct.
func findHeddleFrameField(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Type == reflect.TypeOf(HeddleFrame{}) {
			return v.Field(i)
		}
	}
	return reflect.Value{}
}

// bindFrameValue maps Arrow Table columns to Go struct fields.
// It looks for fields with the `heddle` tag or uses the field name to find matching Arrow columns.
func bindFrameValue(v reflect.Value, table arrow.Table) error {
	// Internal binding logic using reflect.Value
	t := v.Type()
	var frame *HeddleFrame
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Type == reflect.TypeFor[HeddleFrame]() {
			frame = v.Field(i).Addr().Interface().(*HeddleFrame)
			break
		}
	}
	if frame == nil {
		return fmt.Errorf("missing HeddleFrame")
	}
	frame.bind(table)

	schema := table.Schema()
	for i := 0; i < t.NumField(); i++ {
		fValue := v.Field(i)
		if df, ok := fValue.Addr().Interface().(dirtyField); ok {
			name := t.Field(i).Tag.Get("heddle")
			if name == "" {
				name = t.Field(i).Name
			}
			indices := schema.FieldIndices(name)
			if len(indices) > 0 {
				df.bind(table.Column(indices[0]))
				frame.fields = append(frame.fields, df)
			}
		}
	}
	return nil
}
