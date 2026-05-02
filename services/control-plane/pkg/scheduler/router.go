package scheduler

import (
	"fmt"
	"strings"

	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/sdk/go/proto"
)

// Router implements the logical engine of the Control Plane that evaluates DAG topology
// and makes routing decisions based on data locality.
type Router struct {
	registry *locality.DataLocalityRegistry
}

// NewRouter creates a new orchestration router.
func NewRouter(registry *locality.DataLocalityRegistry) *Router {
	return &Router{
		registry: registry,
	}
}

// DecideRoute evaluates the locality of a resource and dispatches a LOCAL or REMOTE ticket.
func (r *Router) DecideRoute(resourceID string, targetWorkerID string) (*proto.FlightTicket, error) {
	metadata, ok := r.registry.Get(resourceID)
	if !ok {
		return nil, fmt.Errorf("resource %s not found in locality registry", resourceID)
	}

	ticket := &proto.FlightTicket{
		ResourceId: resourceID,
	}

	// Logic: If destination worker shares the memory/host, dispatch a LOCAL ticket.
	if metadata.WorkerID == targetWorkerID {
		ticket.RouteType = proto.RouteType_LOCAL
		// Ensure unix prefix for local addresses
		addr := metadata.MemoryHandle
		if !strings.HasPrefix(addr, "unix://") {
			addr = "unix://" + addr
		}
		ticket.Address = addr
	} else {
		// Otherwise, dispatch a REMOTE ticket for external workers.
		ticket.RouteType = proto.RouteType_REMOTE
		addr := metadata.HostAddress
		if !strings.HasPrefix(addr, "grpc://") {
			addr = "grpc://" + addr
		}
		ticket.Address = addr
	}

	return ticket, nil
}
