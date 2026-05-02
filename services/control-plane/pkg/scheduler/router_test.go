package scheduler

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/sdk/go/proto"
	"github.com/stretchr/testify/assert"
)

func TestRouter_DecideRoute(t *testing.T) {
	registry := locality.NewDataLocalityRegistry()
	router := NewRouter(registry)

	resourceID := "shared-data"
	workerA := "worker-local"
	workerB := "worker-remote"

	// Register resource on workerA
	registry.Register(resourceID, locality.LocationMetadata{
		WorkerID:     workerA,
		HostAddress:  "127.0.0.1:50051",
		MemoryHandle: "/tmp/heddle.sock",
	})

	t.Run("LOCAL Route Decision", func(t *testing.T) {
		ticket, err := router.DecideRoute(resourceID, workerA)
		assert.NoError(t, err)
		assert.Equal(t, proto.RouteType_LOCAL, ticket.RouteType)
		assert.Equal(t, "unix:///tmp/heddle.sock", ticket.Address)
		assert.Equal(t, resourceID, ticket.ResourceId)
	})

	t.Run("REMOTE Route Decision", func(t *testing.T) {
		ticket, err := router.DecideRoute(resourceID, workerB)
		assert.NoError(t, err)
		assert.Equal(t, proto.RouteType_REMOTE, ticket.RouteType)
		assert.Equal(t, "grpc://127.0.0.1:50051", ticket.Address)
		assert.Equal(t, resourceID, ticket.ResourceId)
	})

	t.Run("Missing Resource", func(t *testing.T) {
		_, err := router.DecideRoute("ghost", workerA)
		assert.Error(t, err)
	})
}
