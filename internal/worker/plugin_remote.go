package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"

	"github.com/galgotech/heddle-lang/pkg/plugin"
)

type pluginRemote struct {
	stream flight.FlightService_DoExchangeServer

	pluginRegistration plugin.PluginRegistration
	lastHeartbeat      time.Time
	status             string
}

func (p *pluginRemote) PluginRegistration() plugin.PluginRegistration {
	return p.pluginRegistration
}

func (p *pluginRemote) Stream(stream flight.FlightService_DoExchangeServer) {
	p.stream = stream
}

func (p *pluginRemote) HaveStream() bool {
	return p.stream != nil
}

func (p *pluginRemote) Send(ctx context.Context, request plugin.ExecuteStepRequest) error {
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal task to plugin: %w", err)
	}

	return p.stream.Send(&flight.FlightData{DataBody: body})
}

func (p *pluginRemote) Recv() (plugin.ExecuteStepResponse, error) {
	data, err := p.stream.Recv()
	if err != nil {
		return plugin.ExecuteStepResponse{}, err
	}
	var resp plugin.ExecuteStepResponse
	if err := json.Unmarshal(data.DataBody, &resp); err != nil {
		return plugin.ExecuteStepResponse{}, err
	}
	return resp, nil
}

func (p *pluginRemote) LastHeartbeat(hb plugin.Heartbeat) {
	p.lastHeartbeat = hb.Timestamp
	p.status = hb.Status
}
