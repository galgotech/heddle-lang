package registry

import (
	"context"
	"maps"
	"sync"
	"time"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

// NodeRegistry serves as the central state manager for the control plane.
// It tracks connected worker and client nodes, active workflows, and
// maintains a reverse index of capabilities for O(1) scheduling lookups.
type NodeRegistry struct {
	nodesMu sync.RWMutex
	nodes   map[string]*NodeStream

	// capabilities maps a specific capability name to a map of worker IDs
	// that support it, enabling fast capability-based scheduling.
	capabilities map[string]map[string]*NodeStream // capability -> nodeID -> NodeStream

	workflowsMu sync.RWMutex
	workflows   map[string]string // workflowID -> clientID
}

// NewNodeRegistry initializes and returns a new empty NodeRegistry.
func NewNodeRegistry() *NodeRegistry {
	return &NodeRegistry{
		nodes:        make(map[string]*NodeStream),
		capabilities: make(map[string]map[string]*NodeStream),
		workflows:    make(map[string]string),
	}
}

// StartSweeper starts a background goroutine that periodically checks for node heartbeats
// and deregisters nodes that haven't been seen within the timeout period.
func (r *NodeRegistry) StartSweeper(ctx context.Context, interval, timeout time.Duration) {
	logger.L().Info("sweeper started: periodic heartbeat check initialized",
		logger.Component("control-plane"),
		logger.Any("interval", interval),
		logger.Any("timeout", timeout),
	)
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.sweepNodes(timeout)
			case <-ctx.Done():
				logger.L().Debug("sweeper stopped: periodic heartbeat check terminated",
					logger.Component("control-plane"),
				)
				return
			}
		}
	}()
}

func (r *NodeRegistry) sweepNodes(timeout time.Duration) {
	// First collect the IDs of nodes that need to be swept to avoid holding the lock
	// while calling DeregisterNode
	r.nodesMu.RLock()
	threshold := time.Now().Add(-timeout)
	type sweptNode struct {
		id   string
		kind NodeType
	}
	var toSweep []sweptNode
	for id, node := range r.nodes {
		if node.GetLastSeen().Before(threshold) {
			toSweep = append(toSweep, sweptNode{id: id, kind: node.Type})
		}
	}
	r.nodesMu.RUnlock()

	if len(toSweep) > 0 {
		logger.L().Debug("sweeping nodes: found expired nodes to clean up",
			logger.Component("control-plane"),
			logger.Int("count", len(toSweep)),
		)
	}

	for _, n := range toSweep {
		// DeregisterNode automatically calls node.StopStream() as requested
		r.DeregisterNode(n.id)
		if n.kind == NodeTypeWorker {
			logger.L().Warn("node swept: worker node removed due to heartbeat timeout",
				logger.Component("control-plane"),
				logger.WorkerID(n.id),
			)
		} else {
			logger.L().Warn("node swept: client node removed due to heartbeat timeout",
				logger.Component("control-plane"),
				logger.ClientID(n.id),
			)
		}
	}
}

// RegisterNode registers a new node or updates an existing one with the given ID
// and registration data. If the node is a worker and already exists, its previous capability
// entries are removed from the reverse index before re-registration.
func (r *NodeRegistry) RegisterNode(id string, nodeType NodeType, reg models.WorkerRegistration) {
	r.nodesMu.Lock()
	defer r.nodesMu.Unlock()

	isOverwrite := false
	// Clean up capability indexes for existing workers before overwriting.
	if oldNode, ok := r.nodes[id]; ok {
		isOverwrite = true
		if oldNode.Type == NodeTypeWorker {
			oldNode.workerInfo.mu.RLock()
			oldCaps := oldNode.workerInfo.Capabilities
			oldNode.workerInfo.mu.RUnlock()
			for _, c := range oldCaps {
				if r.capabilities[c] != nil {
					delete(r.capabilities[c], id)
					if len(r.capabilities[c]) == 0 {
						delete(r.capabilities, c)
					}
				}
			}
		}
	}

	node := &NodeStream{
		ID:          id,
		Type:        nodeType,
		lastSeen:    time.Now(),
		activeTasks: 0,
		registry:    r,
	}

	if nodeType == NodeTypeWorker {
		node.workerInfo = workerInfo{
			ID:           id,
			Registration: reg,
			Schemas:      make(map[string]schema.StepSchemas),
		}
		node.results = newShardedResultMap()
	}

	r.nodes[id] = node

	var idField logger.Field
	if nodeType == NodeTypeWorker {
		idField = logger.WorkerID(id)
	} else {
		idField = logger.ClientID(id)
	}

	if isOverwrite {
		logger.L().Debug("node registered: overwriting existing node registration",
			logger.Component("control-plane"),
			idField,
			logger.String("type", string(nodeType)),
		)
	} else {
		logger.L().Debug("node registered: added new node to registry",
			logger.Component("control-plane"),
			idField,
			logger.String("type", string(nodeType)),
		)
	}
}

// GetNode retrieves a registered node stream by its ID.
// Returns the NodeStream and a boolean indicating if the node exists.
func (r *NodeRegistry) GetNode(id string) (*NodeStream, bool) {
	r.nodesMu.RLock()
	defer r.nodesMu.RUnlock()

	val, ok := r.nodes[id]
	return val, ok
}

// DeregisterNode removes the node from the registry and cleans up its capability entries in the reverse index.
// It also stops the stream.
func (r *NodeRegistry) DeregisterNode(id string) bool {
	r.nodesMu.Lock()
	val, ok := r.nodes[id]
	if !ok {
		r.nodesMu.Unlock()
		logger.L().Debug("node deregistered: node not found for removal",
			logger.Component("control-plane"),
			logger.String("id", id),
		)
		return false
	}
	delete(r.nodes, id)
	r.nodesMu.Unlock()

	if val.Type == NodeTypeWorker {
		// Clean up the worker from the capability reverse index.
		val.workerInfo.mu.RLock()
		caps := val.workerInfo.Capabilities
		val.workerInfo.mu.RUnlock()

		r.nodesMu.Lock()
		for _, c := range caps {
			if r.capabilities[c] != nil {
				delete(r.capabilities[c], id)
				if len(r.capabilities[c]) == 0 {
					delete(r.capabilities, c)
				}
			}
		}
		r.nodesMu.Unlock()
	}

	val.StopStream()

	var idField logger.Field
	if val.Type == NodeTypeWorker {
		idField = logger.WorkerID(id)
	} else {
		idField = logger.ClientID(id)
	}

	logger.L().Debug("node deregistered: successfully removed node from registry",
		logger.Component("control-plane"),
		idField,
		logger.String("type", string(val.Type)),
	)

	return true
}

// UpdateCapabilities updates the active capabilities of a worker and updates the
// reverse index. It only adds new capabilities to the index; it does not remove
// existing ones that are omitted in the update.
func (r *NodeRegistry) UpdateCapabilities(id string, update models.WorkerCapabilitiesUpdate) bool {
	r.nodesMu.Lock()
	defer r.nodesMu.Unlock()
	val, ok := r.nodes[id]
	if !ok {
		logger.L().Warn("capabilities update failed: worker not found in registry",
			logger.Component("control-plane"),
			logger.String("id", id),
		)
		return false
	}
	if val.Type != NodeTypeWorker {
		logger.L().Warn("capabilities update failed: node is not a worker",
			logger.Component("control-plane"),
			logger.ClientID(id),
			logger.String("type", string(val.Type)),
		)
		return false
	}

	// Capture existing capabilities to avoid duplicate additions to the index.
	val.workerInfo.mu.RLock()
	oldCaps := make(map[string]bool)
	for _, c := range val.workerInfo.Capabilities {
		oldCaps[c] = true
	}
	val.workerInfo.mu.RUnlock()

	val.UpdateCapabilities(update)
	val.LastSeen()

	// Add any newly reported capabilities to the reverse index.
	for _, c := range update.Capabilities {
		if !oldCaps[c] {
			if r.capabilities[c] == nil {
				r.capabilities[c] = make(map[string]*NodeStream)
			}
			r.capabilities[c][id] = val
		}
	}

	logger.L().Debug("capabilities updated: reverse index updated for worker",
		logger.Component("control-plane"),
		logger.WorkerID(id),
		logger.Strings("capabilities", update.Capabilities),
	)

	return true
}

// Heartbeat records a ping from a node, updating its last seen timestamp
// and current load metrics for health monitoring and scheduling decisions.
func (r *NodeRegistry) Heartbeat(id string, load int) bool {
	r.nodesMu.RLock()
	val, ok := r.nodes[id]
	r.nodesMu.RUnlock()

	if !ok {
		logger.L().Warn("heartbeat failed: node not found in registry",
			logger.Component("control-plane"),
			logger.String("id", id),
		)
		return false
	}
	val.UpdateHeartbeat(load, time.Now())

	var idField logger.Field
	if val.Type == NodeTypeWorker {
		idField = logger.WorkerID(id)
	} else {
		idField = logger.ClientID(id)
	}

	logger.L().Debug("heartbeat received: node status updated",
		logger.Component("control-plane"),
		idField,
		logger.Int("load", load),
	)

	return true
}

// FindWorkerByCapability returns an active worker that supports the requested
// capability. It enforces a 15-second freshness threshold on the worker's heartbeat.
// Returns nil if no suitable active worker is found.
func (r *NodeRegistry) FindWorkerByCapability(capability string) *NodeStream {
	r.nodesMu.RLock()
	defer r.nodesMu.RUnlock()

	workers, ok := r.capabilities[capability]
	if !ok || len(workers) == 0 {
		logger.L().Debug("worker discovery: no active worker found for capability",
			logger.Component("control-plane"),
			logger.Capability(capability),
		)
		return nil
	}

	threshold := time.Now().Add(-15 * time.Second)
	for _, w := range workers {
		if w.GetLastSeen().After(threshold) {
			logger.L().Debug("worker discovery: active worker found for capability",
				logger.Component("control-plane"),
				logger.Capability(capability),
				logger.WorkerID(w.GetID()),
			)
			return w
		}
	}

	logger.L().Debug("worker discovery: no active worker found for capability within freshness threshold",
		logger.Component("control-plane"),
		logger.Capability(capability),
	)
	return nil
}

// GetRegistryInfo aggregates and returns the combined step schemas of all workers
// that have sent a heartbeat within the last 30 seconds.
func (r *NodeRegistry) GetRegistryInfo() models.RegistryInfo {
	r.nodesMu.RLock()
	defer r.nodesMu.RUnlock()

	info := models.RegistryInfo{
		Steps: make(map[string]schema.StepSchemas),
	}

	threshold := time.Now().Add(-30 * time.Second)

	activeWorkers := 0
	for _, w := range r.nodes {
		if w.Type == NodeTypeWorker && w.GetLastSeen().After(threshold) {
			activeWorkers++
			w.workerInfo.mu.RLock()
			maps.Copy(info.Steps, w.workerInfo.Schemas)
			w.workerInfo.mu.RUnlock()
		}
	}

	logger.L().Debug("registry info: step schemas aggregated from active workers",
		logger.Component("control-plane"),
		logger.Int("active_workers_count", activeWorkers),
	)

	return info
}

// RegisterWorkflowClient associates a workflow ID with a client ID. This mapping
// is used to route execution results and debug events back to the correct client.
func (r *NodeRegistry) RegisterWorkflowClient(workflowID, clientID string) {
	r.workflowsMu.Lock()
	defer r.workflowsMu.Unlock()
	r.workflows[workflowID] = clientID

	logger.L().Debug("workflow client mapped: registered client for workflow execution",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.ClientID(clientID),
	)
}

// DeregisterWorkflowClient removes the association between a workflow and a client,
// typically called when the workflow completes or fails.
func (r *NodeRegistry) DeregisterWorkflowClient(workflowID string) {
	r.workflowsMu.Lock()
	defer r.workflowsMu.Unlock()
	delete(r.workflows, workflowID)

	logger.L().Debug("workflow client unmapped: removed client association for workflow",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
	)
}

// GetClientIDForWorkflow retrieves the client ID associated with a running workflow.
// Returns the client ID and a boolean indicating if the mapping exists.
func (r *NodeRegistry) GetClientIDForWorkflow(workflowID string) (string, bool) {
	r.workflowsMu.RLock()
	defer r.workflowsMu.RUnlock()
	clientID, ok := r.workflows[workflowID]
	if !ok {
		logger.L().Debug("workflow client lookup: client association not found for workflow",
			logger.Component("control-plane"),
			logger.TraceID(workflowID),
		)
		return "", false
	}

	logger.L().Debug("workflow client lookup: retrieved client for workflow",
		logger.Component("control-plane"),
		logger.TraceID(workflowID),
		logger.ClientID(clientID),
	)
	return clientID, ok
}
