package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/pkg/plugin"
)

func TestValidateSHMPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"Valid path", "/dev/shm/heddle-123.arrow", false},
		{"Outside /dev/shm", "/tmp/heddle-123.arrow", true},
		{"Path traversal", "/dev/shm/../../etc/passwd", true},
		{"Empty path", "", true},
		{"Just /dev/shm/", "/dev/shm/", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSHMPath(tt.path)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

type mockDoActionServer struct {
	flight.FlightService_DoActionServer
	results []*flight.Result
}

func (m *mockDoActionServer) Send(res *flight.Result) error {
	m.results = append(m.results, res)
	return nil
}

func (m *mockDoActionServer) Context() context.Context {
	return context.Background()
}

func TestPluginHeartbeat(t *testing.T) {
	server := NewPluginServer(nil, "/tmp/test.sock")
	namespace := "test-plugin"

	// Pre-register plugin
	server.plugins[namespace] = &pluginRemote{
		pluginRegistration: plugin.PluginRegistration{
			Namespace: namespace,
		},
	}

	hb := plugin.Heartbeat{
		Namespace: namespace,
		Timestamp: time.Now(),
		Status:    "ready",
	}
	body, _ := json.Marshal(hb)

	action := &flight.Action{
		Type: plugin.ActionPluginHeartbeat,
		Body: body,
	}

	mockStream := &mockDoActionServer{}
	err := server.DoAction(action, mockStream)
	require.NoError(t, err)
	assert.Len(t, mockStream.results, 1)
	assert.Equal(t, "OK", string(mockStream.results[0].Body))

	// Verify state update
	val, ok := server.plugins[namespace]
	require.True(t, ok)
	info := val.(*pluginRemote)

	assert.WithinDuration(t, hb.Timestamp, info.lastHeartbeat, time.Second)
	assert.Equal(t, "ready", info.status)
}
