package worker

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/flight"

	"github.com/galgotech/heddle-lang/internal/worker/internal"
	"github.com/galgotech/heddle-lang/internal/worker/std"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

type stepFunc func(ctx context.Context, task plugin.ExecuteStepRequest) (plugin.ExecuteStepResponse, error)

type pluginLocal struct {
	registrySteps      map[string]stepFunc
	pluginRegistration plugin.PluginRegistration
	response           plugin.ExecuteStepResponse
}

func (p *pluginLocal) PluginRegistration() plugin.PluginRegistration {
	return p.pluginRegistration
}

func (p *pluginLocal) Stream(stream flight.FlightService_DoExchangeServer) {
	logger.L().Debug("native stream skipped: plugin is native and runs in-process", logger.Component("plugin-local"), logger.Namespace(p.pluginRegistration.Namespace))
}

func (p *pluginLocal) HaveStream() bool {
	return true
}

func (p *pluginLocal) Send(ctx context.Context, request plugin.ExecuteStepRequest) error {
	capability := fmt.Sprintf("%s.%s", p.pluginRegistration.Namespace, request.StepName)
	logger.L().Debug("native execution initiated: dispatching local request",
		logger.Component("plugin-local"),
		logger.TraceID(request.WorkflowID),
		logger.TaskID(request.TaskID),
		logger.Capability(capability),
	)
	if fn, ok := p.registrySteps[capability]; ok {
		response, err := fn(ctx, request)
		if err != nil {
			logger.L().Error("native execution failed: error executing function",
				logger.Component("plugin-local"),
				logger.TraceID(request.WorkflowID),
				logger.TaskID(request.TaskID),
				logger.Capability(capability),
				logger.Error(err),
			)
			return fmt.Errorf("failed to execute capability %s: %w", capability, err)
		}
		p.response = response
		logger.L().Info("native execution finished: success executing local function",
			logger.Component("plugin-local"),
			logger.TraceID(request.WorkflowID),
			logger.TaskID(request.TaskID),
			logger.Capability(capability),
		)
		return nil
	}
	logger.L().Warn("native execution skipped: capability not registered locally",
		logger.Component("plugin-local"),
		logger.TraceID(request.WorkflowID),
		logger.TaskID(request.TaskID),
		logger.Capability(capability),
	)
	return fmt.Errorf("capability %s not found", capability)
}

func (p *pluginLocal) Recv() (plugin.ExecuteStepResponse, error) {
	logger.L().Debug("native response retrieved: returning local result", logger.Component("plugin-local"), logger.TaskID(p.response.TaskID))
	return p.response, nil
}

func (p *pluginLocal) LastHeartbeat(hb plugin.Heartbeat) {
	logger.L().Debug("native heartbeat received: ignoring heartbeat for in-process plugin", logger.Component("plugin-local"), logger.Namespace(hb.Namespace))
}

func NewNativePlugins() []pluginSdk {
	return []pluginSdk{
		newPluginStdio(),
		newPluginLocal(),
	}
}

func newPluginStdio() *pluginLocal {
	return &pluginLocal{
		registrySteps: map[string]stepFunc{
			"std/io.print": std.ExecutePrint,
		},
		pluginRegistration: plugin.PluginRegistration{
			Namespace: "std/io",
			Language:  "go",
			Version:   "0.0.1",
			Resources: map[string]schema.FieldSchema{},
			Schemas: map[string]schema.StepSchemas{
				"std/io.print": {
					Input: []schema.ColumnSchema{
						{Name: "value", ArrowType: "string"},
					},
					Output: []schema.ColumnSchema{},
				},
			},
		},
	}
}

func newPluginLocal() *pluginLocal {
	return &pluginLocal{
		registrySteps: map[string]stepFunc{
			"__internal.identity":     internal.ExecuteIdentity,
			"__internal.prql":         internal.ExecutePRQL,
			"__internal.data_literal": internal.ExecuteDataLiteral,
		},
		pluginRegistration: plugin.PluginRegistration{
			Namespace: "__internal",
			Language:  "go",
			Version:   "0.0.1",
			Resources: map[string]schema.FieldSchema{},
			Schemas: map[string]schema.StepSchemas{
				"__internal.identity": {
					Config: schema.FieldSchema{},
					Input:  []schema.ColumnSchema{},
					Output: []schema.ColumnSchema{},
				},
				"__internal.prql": {
					Config: schema.FieldSchema{},
					Input:  []schema.ColumnSchema{},
					Output: []schema.ColumnSchema{},
				},
				"__internal.data_literal": {
					Config: schema.FieldSchema{},
					Input:  []schema.ColumnSchema{},
					Output: []schema.ColumnSchema{},
				},
				"__internal.compress": {
					Config: schema.FieldSchema{},
					Input:  []schema.ColumnSchema{},
					Output: []schema.ColumnSchema{},
				},
			},
		},
	}
}
