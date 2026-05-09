package controlplane

import (
	"context"
	"errors"
	"time"
)

// WorkerState represents the availability and health status of an execution node.
type WorkerState string

const (
	// WorkerHealthy indicates the node is responsive and ready to accept task delegations.
	WorkerHealthy WorkerState = "Healthy"
	// WorkerDegraded indicates the node is reachable but operating under resource pressure.
	WorkerDegraded WorkerState = "Degraded"
	// WorkerOffline indicates the node is unreachable or has missed its heartbeat expiration window.
	WorkerOffline WorkerState = "Offline"
)

// Worker represents a stateless execution unit within the Heddle cluster.
type Worker struct {
	ID         string
	Address    string
	UDSAddress string
	Labels     map[string]string
	State      WorkerState
	LastSeenAt time.Time
}

// WorkerRegistry defines the interface for managing the discovery and liveness of execution nodes.
type WorkerRegistry interface {
	Register(id string, address string, udsAddress string, labels map[string]string)
	GetWorker(id string) (*Worker, error)
	Heartbeat(id string) error
	GetHealthyWorker() (*Worker, error)
	GetWorkersByCapability(module, function string) ([]*Worker, error)
	Close()
}

type registerWorkerRequest struct {
	id         string
	address    string
	udsAddress string
	labels     map[string]string
}

type getWorkerRequest struct {
	id     string
	respCh chan getWorkerResponse
}

type getWorkerResponse struct {
	worker *Worker
	err    error
}

type heartbeatRequest struct {
	id     string
	respCh chan error
}

type getHealthyWorkerRequest struct {
	respCh chan getWorkerResponse
}

type getWorkersByCapabilityRequest struct {
	module   string
	function string
	respCh   chan getWorkersByCapabilityResponse
}

type getWorkersByCapabilityResponse struct {
	workers []*Worker
	err     error
}

// DefaultWorkerRegistry provides a lock-free implementation of WorkerRegistry using a manager goroutine.
type DefaultWorkerRegistry struct {
	registerWorkerCh         chan registerWorkerRequest
	getWorkerCh              chan getWorkerRequest
	heartbeatCh              chan heartbeatRequest
	getHealthyWorkerCh       chan getHealthyWorkerRequest
	getWorkersByCapabilityCh chan getWorkersByCapabilityRequest
	stopCh                   chan struct{}
}

func (r *DefaultWorkerRegistry) Register(id string, address string, udsAddress string, labels map[string]string) {
	r.registerWorkerCh <- registerWorkerRequest{id, address, udsAddress, labels}
}

func (r *DefaultWorkerRegistry) GetWorker(id string) (*Worker, error) {
	respCh := make(chan getWorkerResponse, 1)
	r.getWorkerCh <- getWorkerRequest{id, respCh}
	resp := <-respCh
	return resp.worker, resp.err
}

func (r *DefaultWorkerRegistry) Heartbeat(id string) error {
	respCh := make(chan error, 1)
	r.heartbeatCh <- heartbeatRequest{id, respCh}
	return <-respCh
}

func (r *DefaultWorkerRegistry) GetHealthyWorker() (*Worker, error) {
	respCh := make(chan getWorkerResponse, 1)
	r.getHealthyWorkerCh <- getHealthyWorkerRequest{respCh}
	resp := <-respCh
	return resp.worker, resp.err
}

func (r *DefaultWorkerRegistry) GetWorkersByCapability(module, function string) ([]*Worker, error) {
	respCh := make(chan getWorkersByCapabilityResponse, 1)
	r.getWorkersByCapabilityCh <- getWorkersByCapabilityRequest{module, function, respCh}
	resp := <-respCh
	return resp.workers, resp.err
}

func (r *DefaultWorkerRegistry) Close() {
	close(r.stopCh)
}

func (r *DefaultWorkerRegistry) run(ctx context.Context) {
	workers := make(map[string]*Worker)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case req := <-r.registerWorkerCh:
			workers[req.id] = &Worker{
				ID:         req.id,
				Address:    req.address,
				UDSAddress: req.udsAddress,
				Labels:     req.labels,
				State:      WorkerHealthy,
				LastSeenAt: time.Now(),
			}
		case req := <-r.getWorkerCh:
			w, ok := workers[req.id]
			if !ok {
				req.respCh <- getWorkerResponse{nil, errors.New("worker not found")}
			} else {
				req.respCh <- getWorkerResponse{w, nil}
			}
		case req := <-r.heartbeatCh:
			w, ok := workers[req.id]
			if !ok {
				req.respCh <- errors.New("worker not found")
			} else {
				w.LastSeenAt = time.Now()
				w.State = WorkerHealthy
				req.respCh <- nil
			}
		case req := <-r.getHealthyWorkerCh:
			var found *Worker
			for _, w := range workers {
				if time.Since(w.LastSeenAt) <= 30*time.Second && w.State == WorkerHealthy {
					found = w
					break
				}
			}
			if found == nil {
				req.respCh <- getWorkerResponse{nil, errors.New("no healthy workers available")}
			} else {
				req.respCh <- getWorkerResponse{found, nil}
			}
		case req := <-r.getWorkersByCapabilityCh:
			capability := "capability:" + req.module + "." + req.function
			var matches []*Worker
			for _, w := range workers {
				if time.Since(w.LastSeenAt) <= 30*time.Second && w.State == WorkerHealthy {
					if val, ok := w.Labels[capability]; ok && val == "true" {
						matches = append(matches, w)
					}
				}
			}
			if len(matches) == 0 {
				req.respCh <- getWorkersByCapabilityResponse{nil, errors.New("no healthy workers found with capability: " + capability)}
			} else {
				req.respCh <- getWorkersByCapabilityResponse{matches, nil}
			}
		case <-ticker.C:
			// Cleanup or mark offline? The GetHealthyWorker already checks timestamp.
			// Could mark Offline here for visibility.
			for _, w := range workers {
				if time.Since(w.LastSeenAt) > 60*time.Second {
					w.State = WorkerOffline
				}
			}
		case <-r.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// NewWorkerRegistry constructs a fresh instance of the default worker registry.
func NewWorkerRegistry() WorkerRegistry {
	r := &DefaultWorkerRegistry{
		registerWorkerCh:         make(chan registerWorkerRequest, 10),
		getWorkerCh:              make(chan getWorkerRequest, 10),
		heartbeatCh:              make(chan heartbeatRequest, 10),
		getHealthyWorkerCh:       make(chan getHealthyWorkerRequest, 10),
		getWorkersByCapabilityCh: make(chan getWorkersByCapabilityRequest, 10),
		stopCh:                   make(chan struct{}),
	}
	go r.run(context.Background())
	return r
}
