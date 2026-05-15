package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/signal"
	"reflect"
	"runtime"
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
	baseplugin "github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

// ResourceRegistration maintains metadata and the execution handle for a Heddle Resource.
// It allows the plugin to expose custom infrastructure or stateful components to the Heddle DSL.
type ResourceRegistration struct {
	Name           string
	ResourceSchema *schema.ResourceAndConfigSchema
	ResourceType   reflect.Type
	Resource       Resource
	Documentation  string
	SourceCode     string
	SourceFile     string
	SourceLine     int
}

// StepRegistration stores the execution contract for a Heddle Step.
// It captures the function signature, inferred JSON schemas for configuration,
// and the mapping between Arrow schemas and Go struct types.
type StepRegistration struct {
	Name         string
	ConfigSchema *schema.ResourceAndConfigSchema // JSON schema inferred from the configuration struct for DSL-side validation
	ConfigType   reflect.Type
	InputType    reflect.Type
	OutputType   reflect.Type
	Func         reflect.Value
	InputSchema  *schema.FrameSchema
	OutputSchema *schema.FrameSchema

	Documentation string
	SourceCode    string
	SourceFile    string
	SourceLine    int

	// Pre-calculated indices for optimized initialization
	inputHeddleFrameIndex  int
	outputHeddleFrameIndex int
	inputFieldsIndex       []int
	outputFieldsIndex      []int
}

// NewInputOutput initializes both input and output frames using pre-calculated indices.
// It pre-populates the HeddleFrame schema and initializes any nil field pointers.
func (s *StepRegistration) NewInputOutput() (reflect.Value, reflect.Value) {
	inputVal := reflect.New(s.InputType.Elem())
	outputVal := reflect.New(s.OutputType.Elem())

	s.initFrame(inputVal, s.inputFieldsIndex)
	s.initFrame(outputVal, s.outputFieldsIndex)

	return inputVal, outputVal
}

func (s *StepRegistration) initFrame(val reflect.Value, indices []int) {
	v := val.Elem()

	// Initialize all pointer fields
	for _, i := range indices {
		f := v.Field(i)
		f.Set(reflect.New(f.Type().Elem()))
	}
}

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
	typ := reflect.TypeOf(resource)

	if typ.Kind() != reflect.Pointer {
		return fmt.Errorf("resource %q must be a pointer to a struct", name)
	}

	typ = typ.Elem()
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("resource %q must be a pointer to a struct", name)
	}

	resourceSchema, err := ExtractResourceAndConfigSchema(typ)
	if err != nil {
		logger.L().Error("Failed to extract resource schema", zap.String("resource", name), zap.Error(err))
		return fmt.Errorf("resource %q config: %w", name, err)
	}

	var fnPtr uintptr
	if v := reflect.ValueOf(resource).MethodByName("Start"); v.IsValid() {
		fnPtr = v.Pointer()
	}
	doc, code, file, line := extractMetadata(fnPtr)

	p.resources[name] = ResourceRegistration{
		Name:           name,
		ResourceSchema: resourceSchema,
		ResourceType:   typ,
		Resource:       resource,
		Documentation:  doc,
		SourceCode:     code,
		SourceFile:     file,
		SourceLine:     line,
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

	// Ensure the function signature matches one of the expected contracts:
	// func(context.Context, TConfig, TInput, TOutput) error
	if typ.NumIn() != 4 || typ.NumOut() != 1 {
		return fmt.Errorf("step %q must have signature func(ctx, config, input, output) error", name)
	}

	configType := typ.In(1)
	inputType := typ.In(2)
	outputType := typ.In(3)

	configSchema, err := ExtractResourceAndConfigSchema(configType)
	if err != nil {
		logger.L().Error("Failed to extract step config schema", zap.String("step", name), zap.Error(err))
		return fmt.Errorf("step %q config: %w", name, err)
	}

	inputSchema, err := ExtractInputOutputSchema(inputType)
	if err != nil {
		logger.L().Error("Failed to extract step input schema", zap.String("step", name), zap.Error(err))
		return fmt.Errorf("step %q input: %w", name, err)
	}

	outputSchema, err := ExtractInputOutputSchema(outputType)
	if err != nil {
		logger.L().Error("Failed to extract step output schema", zap.String("step", name), zap.Error(err))
		return fmt.Errorf("step %q output: %w", name, err)
	}

	var inputHeddleFrameIndex int
	inputFieldsIndex := []int{}
	inType := inputType.Elem()
	for i := 0; i < inType.NumField(); i++ {
		if inType.Field(i).Type != reflect.TypeFor[HeddleFrame]() {
			inputFieldsIndex = append(inputFieldsIndex, i)
		} else {
			inputHeddleFrameIndex = i
		}
	}

	var outputHeddleFrameIndex int
	outputFieldsIndex := []int{}
	outType := outputType.Elem()
	for i := 0; i < outType.NumField(); i++ {
		if outType.Field(i).Type != reflect.TypeFor[HeddleFrame]() {
			outputFieldsIndex = append(outputFieldsIndex, i)
		} else {
			outputHeddleFrameIndex = i
		}
	}

	doc, code, file, line := extractMetadata(reflect.ValueOf(fn).Pointer())

	logger.L().Debug("Registering step", zap.String("name", name))
	p.steps[name] = StepRegistration{
		Name:                   name,
		Func:                   reflect.ValueOf(fn),
		ConfigSchema:           configSchema,
		ConfigType:             configType,
		InputSchema:            inputSchema,
		InputType:              inputType,
		OutputSchema:           outputSchema,
		OutputType:             outputType,
		Documentation:          doc,
		SourceCode:             code,
		SourceFile:             file,
		SourceLine:             line,
		inputHeddleFrameIndex:  inputHeddleFrameIndex,
		outputHeddleFrameIndex: outputHeddleFrameIndex,
		inputFieldsIndex:       inputFieldsIndex,
		outputFieldsIndex:      outputFieldsIndex,
	}

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
		logger.L().Info("Preparing plugin registration", zap.Int("steps", len(p.steps)), zap.Int("resources", len(p.resources)))
		capabilities := make([]string, 0, len(p.steps)+len(p.resources))
		schemas := make(map[string]schema.StepSchemas)
		for name, step := range p.steps {
			capName := fmt.Sprintf("%s.%s", p.Namespace, name)
			capabilities = append(capabilities, capName)
			schemas[capName] = schema.StepSchemas{
				Config:        step.ConfigSchema,
				Input:         step.InputSchema,
				Output:        step.OutputSchema,
				Documentation: step.Documentation,
				SourceCode:    step.SourceCode,
				SourceFile:    step.SourceFile,
				SourceLine:    step.SourceLine,
			}
		}

		for name := range p.resources {
			capabilities = append(capabilities, fmt.Sprintf("%s.resource.%s", p.Namespace, name))
		}

		resources := make(map[string]*schema.ResourceAndConfigSchema)
		for name, res := range p.resources {
			resources[name] = res.ResourceSchema
		}

		reg := baseplugin.PluginRegistration{
			Namespace:    p.Namespace,
			Language:     p.Language,
			Version:      "0.1.0",
			Capabilities: capabilities,
			Schemas:      schemas,
			Resources:    resources,
		}
		regBody, err := json.Marshal(reg)
		if err != nil {
			logger.L().Info("Failed to marshal plugin registration...", zap.String("target", target))
			return err
		}

		// Submit registration via Arrow Flight DoAction.
		// This notifies the Worker of the plugin's namespace and step capabilities.
		res, err := client.DoAction(ctx, &flight.Action{
			Type: baseplugin.ActionRegisterPlugin,
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
		logger.L().Debug("Plugin registered", zap.String("capabilities", fmt.Sprintf("%v", reg.Capabilities)))
		logger.L().Debug("Plugin registered", zap.String("resources", fmt.Sprintf("%v", reg.Resources)))
		logger.L().Debug("Plugin registered", zap.String("schemas", fmt.Sprintf("%v", reg.Schemas)))

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
				Type: baseplugin.ActionPluginHeartbeat,
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
	targetStep, ok := p.steps[req.StepName]
	if !ok {
		return ExecuteStepResponse{
			TaskID:       req.TaskID,
			Status:       StepResponseError,
			ErrorMessage: fmt.Sprintf("step %s not found", req.StepName),
		}
	}

	// 2. Hydrate the step configuration from the provided JSON.
	configType := targetStep.ConfigType
	if configType.Kind() == reflect.Pointer {
		return ExecuteStepResponse{
			TaskID:       req.TaskID,
			Status:       StepResponseError,
			ErrorMessage: "step config must be a struct",
		}
	}

	configVal := reflect.New(configType)
	if req.ConfigJSON != "" {
		if err := json.Unmarshal([]byte(req.ConfigJSON), configVal.Interface()); err != nil {
			return ExecuteStepResponse{
				TaskID:       req.TaskID,
				Status:       StepResponseError,
				ErrorMessage: fmt.Errorf("failed to unmarshal config: %w", err).Error(),
			}
		}
	}

	// 3. Prepare the Input Frame using Zero-Copy SHM access.
	columns := make(map[string]arrow.Array)
	for fieldName, path := range req.InputHandles {
		arr, err := locality.ReadArrowArrayFromPath(path)
		if err != nil {
			logger.L().Error("Failed to read input from SHM", zap.Error(err), zap.String("path", path))
		} else {
			columns[fieldName] = arr
			defer arr.Release()
		}
	}

	inputVal, outputVal := targetStep.NewInputOutput()
	if len(columns) > 0 {
		if err := bind(inputVal, targetStep.inputFieldsIndex, columns); err != nil {
			return ExecuteStepResponse{
				TaskID:       req.TaskID,
				Status:       StepResponseError,
				ErrorMessage: fmt.Sprintf("failed to bind input frame: %v", err),
			}
		}
	}

	results := targetStep.Func.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		configVal.Elem(),
		inputVal,
		outputVal,
	})

	// 5. Handle output results and commit data to SHM.
	errResult := results[0]
	if !errResult.IsNil() {
		return ExecuteStepResponse{
			TaskID:       req.TaskID,
			Status:       StepResponseError,
			ErrorMessage: errResult.Interface().(error).Error(),
		}
	}

	// Check if the output is a VoidFrame (explicitly no-data).
	if targetStep.OutputType == reflect.TypeFor[VoidFrame]() {
		return ExecuteStepResponse{
			TaskID: req.TaskID,
			Status: StepResponseSuccess,
		}
	}

	outputHandles := make(map[string]string)
	dirtyHandles := make(map[string]string)

	vVal := outputVal.Elem()
	t := vVal.Type()

	for _, i := range targetStep.outputFieldsIndex {
		fValue := vVal.Field(i)
		fieldPtr := fValue.Interface()
		name := t.Field(i).Name

		var arr arrow.Array
		var dirt []uint64

		switch dataFrame := fieldPtr.(type) {
		case *Int8:
			arr = dataFrame.arrayInt8
			dirt = dataFrame.dirt
		case *Int16:
			arr = dataFrame.arrayInt16
			dirt = dataFrame.dirt
		case *Int32:
			arr = dataFrame.arrayInt32
			dirt = dataFrame.dirt
		case *Int64:
			arr = dataFrame.arrayInt64
			dirt = dataFrame.dirt
		case *Uint8:
			arr = dataFrame.arrayUint8
			dirt = dataFrame.dirt
		case *Uint16:
			arr = dataFrame.arrayUint16
			dirt = dataFrame.dirt
		case *Uint32:
			arr = dataFrame.arrayUint32
			dirt = dataFrame.dirt
		case *Uint64:
			arr = dataFrame.arrayUint64
			dirt = dataFrame.dirt
		case *Float32:
			arr = dataFrame.arrayFloat32
			dirt = dataFrame.dirt
		case *Float64:
			arr = dataFrame.arrayFloat64
			dirt = dataFrame.dirt
		case *Bool:
			arr = dataFrame.arrayBool
			dirt = dataFrame.dirt
		case *String:
			arr = dataFrame.arrayString
			dirt = dataFrame.dirt
		default:
			logger.L().Error("unsupported output field type", zap.String("type", fValue.Type().String()))
			return ExecuteStepResponse{
				TaskID:       req.TaskID,
				Status:       StepResponseError,
				ErrorMessage: fmt.Sprintf("unsupported output field type %s", fValue.Type()),
			}
		}

		if arr != nil && !reflect.ValueOf(arr).IsNil() {
			path, err := locality.WriteArrowArrayOnlyToShm(arr)
			if err != nil {
				return ExecuteStepResponse{
					TaskID:       req.TaskID,
					Status:       StepResponseError,
					ErrorMessage: fmt.Sprintf("failed to write output frame: %v", err),
				}
			} else {
				outputHandles[name] = path
			}

			hasDirty := false
			for _, d := range dirt {
				if d != 0 {
					hasDirty = true
					break
				}
			}
			if hasDirty {
				dp, err := locality.WriteDirtyToShm(dirt)
				if err != nil {
					logger.L().Error("Failed to write dirty bits to SHM", zap.Error(err))
				} else {
					dirtyHandles[name] = dp
				}
			}
		}
	}

	return ExecuteStepResponse{
		TaskID:        req.TaskID,
		Status:        StepResponseSuccess,
		OutputHandles: outputHandles,
		DirtyHandles:  dirtyHandles,
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

// bind maps Arrow Table columns to Go struct fields.
func bind(reflectValue reflect.Value, fieldIndices []int, columns map[string]arrow.Array) error {
	if reflectValue.Kind() != reflect.Pointer {
		return fmt.Errorf("type %v is not a pointer", reflectValue.Type())
	}

	v := reflectValue.Elem()
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("type %v is not a struct", v.Type())
	}

	var numRows int = -1
	for _, arr := range columns {
		if numRows == -1 {
			numRows = arr.Len()
		} else if numRows != arr.Len() {
			return fmt.Errorf("inconsistent column lengths")
		}
	}
	if numRows == -1 {
		numRows = 0
	}

	t := v.Type()
	for _, i := range fieldIndices {
		fValue := v.Field(i)
		fieldPtr := fValue.Interface()

		name := t.Field(i).Name
		arr := columns[name]
		if arr == nil {
			return fmt.Errorf("column %q is required but missing", name)
		}

		switch df := fieldPtr.(type) {
		case *Int8:
			df.arrayInt8 = arr.(*array.Int8)
			df.dirt = []uint64{}
		case *Int16:
			df.arrayInt16 = arr.(*array.Int16)
			df.dirt = []uint64{}
		case *Int32:
			df.arrayInt32 = arr.(*array.Int32)
			df.dirt = []uint64{}
		case *Int64:
			df.arrayInt64 = arr.(*array.Int64)
			df.dirt = []uint64{}
		case *Uint8:
			df.arrayUint8 = arr.(*array.Uint8)
			df.dirt = []uint64{}
		case *Uint16:
			df.arrayUint16 = arr.(*array.Uint16)
			df.dirt = []uint64{}
		case *Uint32:
			df.arrayUint32 = arr.(*array.Uint32)
			df.dirt = []uint64{}
		case *Uint64:
			df.arrayUint64 = arr.(*array.Uint64)
			df.dirt = []uint64{}
		case *Float32:
			df.arrayFloat32 = arr.(*array.Float32)
			df.dirt = []uint64{}
		case *Float64:
			df.arrayFloat64 = arr.(*array.Float64)
			df.dirt = []uint64{}
		case *Bool:
			df.arrayBool = arr.(*array.Boolean)
			df.dirt = []uint64{}
		case *String:
			df.arrayString = arr.(*array.String)
			df.dirt = []uint64{}
		default:
			return fmt.Errorf("field name '%s' has unsupported type %v", name, fValue.Type())
		}
	}

	return nil
}

func extractMetadata(fnPtr uintptr) (doc string, code string, file string, line int) {
	f := runtime.FuncForPC(fnPtr)
	if f == nil {
		return
	}
	file, line = f.FileLine(fnPtr)

	// Try to read the source file
	data, err := os.ReadFile(file)
	if err != nil {
		return
	}

	fset := token.NewFileSet()
	// Parse the file to get AST and comments
	node, err := parser.ParseFile(fset, file, data, parser.ParseComments)
	if err != nil {
		return
	}

	for _, decl := range node.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			// Check if this function corresponds to our line
			startLine := fset.Position(d.Pos()).Line
			endLine := fset.Position(d.End()).Line
			if startLine <= line && endLine >= line {
				if d.Doc != nil {
					doc = d.Doc.Text()
				}
				// Extract source code of the function
				start := fset.Position(d.Pos()).Offset
				end := fset.Position(d.End()).Offset
				code = string(data[start:end])
				return
			}
		case *ast.GenDecl:
			// Handle types (Resources)
			for _, spec := range d.Specs {
				if tSpec, ok := spec.(*ast.TypeSpec); ok {
					startLine := fset.Position(tSpec.Pos()).Line
					endLine := fset.Position(tSpec.End()).Line
					if startLine <= line && endLine >= line {
						if d.Doc != nil {
							doc = d.Doc.Text()
						}
						// Extract source code of the type declaration
						start := fset.Position(d.Pos()).Offset
						end := fset.Position(d.End()).Offset
						code = string(data[start:end])
						return
					}
				}
			}
		}
	}
	return
}
