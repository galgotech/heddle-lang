package worker

import (
	"context"

	"github.com/apache/arrow/go/v18/arrow/flight"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/internal/worker/internal"
	"github.com/galgotech/heddle-lang/internal/worker/std"
	"github.com/galgotech/heddle-lang/pkg/plugin"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

type stepFunc func(ctx context.Context, task models.StepExecutionTask, registry *locality.DataLocalityRegistry) (models.TaskResult, error)

type pluginLocal struct {
	registry           map[string]stepFunc
	pluginRegistration plugin.PluginRegistration
}

func (p *pluginLocal) PluginRegistration() plugin.PluginRegistration {
	return p.pluginRegistration
}

func (p *pluginLocal) Stream(stream flight.FlightService_DoExchangeServer) {
}

func (p *pluginLocal) HaveStream() bool {
	return true
}

func (p *pluginLocal) Send(request plugin.ExecuteStepRequest) error {
	return nil
}

func (p *pluginLocal) Recv() (*flight.FlightData, error) {
	return nil, nil
}

func (p *pluginLocal) LastHeartbeat(hb plugin.Heartbeat) {

}

var registryStd = map[string]stepFunc{
	"std/io.print": std.ExecutePrint,
}

var pluginStdIo = &pluginLocal{
	registry: registryStd,
	pluginRegistration: plugin.PluginRegistration{
		Namespace: "std/io",
		Language:  "go",
		Version:   "0.0.1",
		Capabilities: func() []string {
			caps := []string{}
			for k := range registryStd {
				caps = append(caps, k)
			}
			return caps
		}(),
		Resources: map[string]*schema.ResourceAndConfigSchema{},
		Schemas: map[string]schema.StepSchemas{
			"std/io.print": schema.StepSchemas{
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

var registryInternal = map[string]stepFunc{
	"__internal.identity":     internal.ExecuteIdentity,
	"__internal.prql":         internal.ExecutePRQL,
	"__internal.data_literal": internal.ExecuteDataLiteral,
	"__internal.compress":     internal.ExecuteCompress,
}

var pluginInternal = &pluginLocal{
	registry: registryInternal,
	pluginRegistration: plugin.PluginRegistration{
		Namespace: "__internal",
		Language:  "go",
		Version:   "0.0.1",
		Capabilities: func() []string {
			caps := []string{}
			for k := range registryInternal {
				caps = append(caps, k)
			}
			return caps
		}(),
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
