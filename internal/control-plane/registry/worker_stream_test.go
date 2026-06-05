package registry

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

func TestWorkerInfo_UpdateCapabilities(t *testing.T) {
	info := &workerInfo{
		ID:           "worker-1",
		Capabilities: []string{"cap.1"},
	}

	// Update with new and duplicate capabilities
	update := models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"cap.1", "cap.2"},
		Schemas: map[string]schema.StepSchemas{
			"cap.2": {
				Input: schema.FrameSchema{},
			},
		},
	}

	info.UpdateCapabilities(update)

	assert.ElementsMatch(t, []string{"cap.1", "cap.2"}, info.Capabilities)
	assert.Len(t, info.Schemas, 1)
	assert.Contains(t, info.Schemas, "cap.2")
}

func TestWorkerInfo_GetSchemaForCapability(t *testing.T) {
	// 1. Get schema when schemas map is nil
	info := &workerInfo{ID: "worker-1"}
	_, ok := info.GetSchemaForCapability("cap.1")
	assert.False(t, ok)

	// 2. Get schema when capability exists
	info.Schemas = map[string]schema.StepSchemas{
		"cap.1": {
			Input: schema.FrameSchema{},
		},
	}
	_, ok = info.GetSchemaForCapability("cap.1")
	assert.True(t, ok)

	// 3. Get schema when capability does not exist
	_, ok = info.GetSchemaForCapability("cap.none")
	assert.False(t, ok)
}

func TestWorkerStream_BasicAccessors(t *testing.T) {
	w := &WorkerStream{
		workerInfo: workerInfo{
			ID: "worker-1",
		},
	}

	assert.Equal(t, "worker-1", w.GetID())

	// Test UpdateCapabilities and GetSchemaForCapability wrappers
	update := models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"cap.1"},
		Schemas: map[string]schema.StepSchemas{
			"cap.1": {
				Input: schema.FrameSchema{},
			},
		},
	}
	w.UpdateCapabilities(update)

	// Test LastSeen
	assert.Zero(t, w.lastSeen)
	w.LastSeen()
	assert.NotZero(t, w.lastSeen)
	assert.WithinDuration(t, time.Now(), w.lastSeen, 2*time.Second)

	// Test StopStream
	mockStream := &mockExchangeServer{}
	w.stream = mockStream
	w.StopStream()
	assert.Nil(t, w.stream)
}

func TestWorkerStream_ProcessStream(t *testing.T) {
	w := &WorkerStream{
		workerInfo: workerInfo{
			ID: "worker-1",
		},
	}

	// 1. Process nil stream
	assert.False(t, w.ProcessStream(nil))

	// 2. Process active stream, send data, metadata, invalid json
	recvChan := make(chan *flight.FlightData, 10)
	errChan := make(chan error, 1)

	mockStream := &mockExchangeServer{
		recvChan: recvChan,
		errChan:  errChan,
	}

	assert.True(t, w.ProcessStream(mockStream))
	assert.Equal(t, mockStream, w.stream)

	// Send nil response to test warnings (should be ignored, loop continues)
	recvChan <- nil

	// Send control message (with AppMetadata)
	recvChan <- &flight.FlightData{
		AppMetadata: []byte("control-signal"),
	}

	// Send valid TaskResult
	res := models.TaskResult{
		TaskID: "task-123",
		Status: models.TaskStatusSuccess,
	}
	resBody, _ := json.Marshal(res)
	recvChan <- &flight.FlightData{
		DataBody: resBody,
	}

	// Send invalid TaskResult json to test logger Warn
	recvChan <- &flight.FlightData{
		DataBody: []byte("invalid-json{"),
	}

	// Wait briefly for goroutine to consume
	time.Sleep(50 * time.Millisecond)

	// Clean up by closing errChan to exit goroutine
	close(errChan)
}
