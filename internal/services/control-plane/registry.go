package control_plane

import (
	"sync"
	"time"

	"github.com/galgotech/heddle-lang/internal/services/models"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

type WorkerInfo struct {
	ID           string
	Registration models.WorkerRegistration
	Capabilities []string
	Schemas      map[string]schema.StepSchemas
	LastSeen     time.Time
	ActiveTasks  int
}

type WorkerRegistry struct {
	workers sync.Map // map[string]*WorkerInfo
}

func (r *WorkerRegistry) Register(id string, reg models.WorkerRegistration) {
	r.workers.Store(id, &WorkerInfo{
		ID:           id,
		Registration: reg,
		LastSeen:     time.Now(),
	})
}

func (r *WorkerRegistry) UpdateCapabilities(id string, update models.WorkerCapabilitiesUpdate) bool {
	val, ok := r.workers.Load(id)
	if !ok {
		return false
	}
	info := val.(*WorkerInfo)

	// Initialize schemas if nil
	if info.Schemas == nil {
		info.Schemas = make(map[string]schema.StepSchemas)
	}

	// Merge unique capabilities and update schemas
	capsMap := make(map[string]bool)
	for _, c := range info.Capabilities {
		capsMap[c] = true
	}
	for _, c := range update.Capabilities {
		if !capsMap[c] {
			info.Capabilities = append(info.Capabilities, c)
			capsMap[c] = true
		}
	}

	// Update schemas
	for k, v := range update.Schemas {
		info.Schemas[k] = v
	}

	info.LastSeen = time.Now()
	return true
}

func (r *WorkerRegistry) Heartbeat(id string, load int) bool {
	val, ok := r.workers.Load(id)
	if !ok {
		return false
	}
	info := val.(*WorkerInfo)
	info.LastSeen = time.Now()
	info.ActiveTasks = load
	return true
}

func (r *WorkerRegistry) GetHealthyWorkers() []*WorkerInfo {
	var healthy []*WorkerInfo
	threshold := time.Now().Add(-15 * time.Second)

	r.workers.Range(func(key, value interface{}) bool {
		info := value.(*WorkerInfo)
		if info.LastSeen.After(threshold) {
			healthy = append(healthy, info)
		}
		return true
	})
	return healthy
}

func (r *WorkerRegistry) FindWorkerForStep(capability string) *WorkerInfo {
	var best *WorkerInfo
	threshold := time.Now().Add(-15 * time.Second)

	r.workers.Range(func(key, value interface{}) bool {
		info := value.(*WorkerInfo)
		if info.LastSeen.After(threshold) {
			for _, cap := range info.Capabilities {
				if cap == capability {
					if best == nil || info.ActiveTasks < best.ActiveTasks {
						best = info
					}
					break
				}
			}
		}
		return true
	})
	return best
}

func (r *WorkerRegistry) GetRegistryInfo() models.RegistryInfo {
	info := models.RegistryInfo{
		Steps: make(map[string]schema.StepSchemas),
	}

	threshold := time.Now().Add(-30 * time.Second)

	r.workers.Range(func(key, value interface{}) bool {
		w := value.(*WorkerInfo)
		if w.LastSeen.After(threshold) {
			for cap, schemas := range w.Schemas {
				info.Steps[cap] = schemas
			}
		}
		return true
	})

	return info
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{}
}
