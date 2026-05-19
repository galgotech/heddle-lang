package worker

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/flight"

	"github.com/galgotech/heddle-lang/internal/worker/internal"
	"github.com/galgotech/heddle-lang/internal/worker/std"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
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
}

func (p *pluginLocal) HaveStream() bool {
	return true
}

func (p *pluginLocal) Send(ctx context.Context, request plugin.ExecuteStepRequest) error {
	capability := fmt.Sprintf("%s.%s", p.pluginRegistration.Namespace, request.StepName)
	if fn, ok := p.registrySteps[capability]; ok {
		response, err := fn(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to execute capability %s: %w", capability, err)
		}
		p.response = response
		return nil
	}
	return fmt.Errorf("capability %s not found", capability)
}

func (p *pluginLocal) Recv() (plugin.ExecuteStepResponse, error) {
	return p.response, nil
}

func (p *pluginLocal) LastHeartbeat(hb plugin.Heartbeat) {

}

func NewNativePlugins(registry *locality.DataLocalityRegistry) []pluginSdk {
	return []pluginSdk{
		newPluginStdio(registry),
		newPluginLocal(registry),
	}
}

func newPluginStdio(registry *locality.DataLocalityRegistry) *pluginLocal {
	return &pluginLocal{
		registrySteps: map[string]stepFunc{
			"std/io.print": std.ExecutePrint,
		},
		pluginRegistration: plugin.PluginRegistration{
			Namespace:    "std/io",
			Language:     "go",
			Version:      "0.0.1",
			Capabilities: []string{"std/io.print"},
			Resources:    map[string]*schema.ResourceAndConfigSchema{},
			Schemas: map[string]schema.StepSchemas{
				"std/io.print": {
					Input: &schema.FrameSchema{
						Fields: []schema.FrameSchemaField{
							{Name: "value", ArrowType: "string"},
						},
						IsVoid: false,
					},
					Output: &schema.FrameSchema{
						Fields: []schema.FrameSchemaField{},
						IsVoid: true,
					},
				},
			},
		},
	}
}

func newPluginLocal(registry *locality.DataLocalityRegistry) *pluginLocal {
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
			Capabilities: []string{
				"__internal.identity",
				"__internal.prql",
				"__internal.data_literal",
			},
			Resources: map[string]*schema.ResourceAndConfigSchema{},
			Schemas: map[string]schema.StepSchemas{
				"__internal.identity": {
					Input:  nil,
					Output: nil,
				},
				"__internal.prql": {
					Input:  nil,
					Output: nil,
				},
				"__internal.data_literal": {
					Input: &schema.FrameSchema{
						Fields: []schema.FrameSchemaField{},
						IsVoid: true,
					},
					Output: nil,
				},
				"__internal.compress": {
					Input:  nil,
					Output: nil,
				},
			},
		},
	}
}
