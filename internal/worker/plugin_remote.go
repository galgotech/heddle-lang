package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"

	"github.com/galgotech/heddle-lang/pkg/logger"
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
	logger.L().Info("remote stream connected: exchange stream established for plugin",
		logger.Component("plugin-remote"),
		logger.Namespace(p.pluginRegistration.Namespace),
	)
	p.stream = stream
}

func (p *pluginRemote) HaveStream() bool {
	return p.stream != nil
}

func (p *pluginRemote) Send(ctx context.Context, request plugin.ExecuteStepRequest) error {
	logger.L().Debug("remote execution initiated: sending request payload to plugin socket",
		logger.Component("plugin-remote"),
		logger.TraceID(request.WorkflowID),
		logger.TaskID(request.TaskID),
		logger.Namespace(p.pluginRegistration.Namespace),
	)
	body, err := json.Marshal(request)
	if err != nil {
		logger.L().Error("remote execution failed: failed to marshal task request",
			logger.Component("plugin-remote"),
			logger.TraceID(request.WorkflowID),
			logger.TaskID(request.TaskID),
			logger.Namespace(p.pluginRegistration.Namespace),
			logger.Error(err),
		)
		return fmt.Errorf("failed to marshal task to plugin: %w", err)
	}

	if err := p.stream.Send(&flight.FlightData{DataBody: body}); err != nil {
		logger.L().Error("remote execution failed: failed to transmit task payload to plugin stream",
			logger.Component("plugin-remote"),
			logger.TraceID(request.WorkflowID),
			logger.TaskID(request.TaskID),
			logger.Namespace(p.pluginRegistration.Namespace),
			logger.Error(err),
		)
		return err
	}
	return nil
}

func (p *pluginRemote) Recv() (plugin.ExecuteStepResponse, error) {
	logger.L().Debug("remote response wait: listening for response from plugin stream",
		logger.Component("plugin-remote"),
		logger.Namespace(p.pluginRegistration.Namespace),
	)
	data, err := p.stream.Recv()
	if err != nil {
		logger.L().Error("remote response failed: failed to receive data from plugin stream",
			logger.Component("plugin-remote"),
			logger.Namespace(p.pluginRegistration.Namespace),
			logger.Error(err),
		)
		return plugin.ExecuteStepResponse{}, err
	}
	var resp plugin.ExecuteStepResponse
	if err := json.Unmarshal(data.DataBody, &resp); err != nil {
		logger.L().Error("remote response failed: failed to unmarshal result payload",
			logger.Component("plugin-remote"),
			logger.Namespace(p.pluginRegistration.Namespace),
			logger.Error(err),
		)
		return plugin.ExecuteStepResponse{}, err
	}
	logger.L().Info("remote response received: successfully executed step and returned response",
		logger.Component("plugin-remote"),
		logger.TaskID(resp.TaskID),
		logger.Namespace(p.pluginRegistration.Namespace),
	)
	return resp, nil
}

func (p *pluginRemote) LastHeartbeat(hb plugin.Heartbeat) {
	logger.L().Debug("remote heartbeat registered: update heartbeat timestamp",
		logger.Component("plugin-remote"),
		logger.Namespace(p.pluginRegistration.Namespace),
		logger.String("status", hb.Status),
	)
	p.lastHeartbeat = hb.Timestamp
	p.status = hb.Status
}
