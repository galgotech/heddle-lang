package locality

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataLocalityRegistry(t *testing.T) {
	registry := NewDataLocalityRegistry()

	t.Run("Register and Get", func(t *testing.T) {
		resourceID := "test-resource-1"
		metadata := LocationMetadata{
			WorkerID:     "worker-a",
			HostAddress:  "127.0.0.1:50051",
			MemoryHandle: "/tmp/heddle-shm-1",
		}

		registry.Register(resourceID, metadata)

		got, ok := registry.Get(resourceID)
		assert.True(t, ok)
		assert.Equal(t, metadata, got)
	})

	t.Run("Get Non-Existent", func(t *testing.T) {
		_, ok := registry.Get("ghost-resource")
		assert.False(t, ok)
	})

	t.Run("Concurrency Test", func(t *testing.T) {
		const count = 100
		for i := 0; i < count; i++ {
			go func(id int) {
				registry.Register("res", LocationMetadata{WorkerID: "w"})
			}(i)
		}
		// Basic check that it doesn't panic
		_, _ = registry.Get("res")
	})
}
