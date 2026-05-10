package control_plane

import (
	"sync"
	"time"

	"github.com/galgotech/heddle-lang/internal/services/models"
)

type WorkerInfo struct {
	ID           string
	Registration models.WorkerRegistration
	LastSeen     time.Time
	ActiveTasks  int
}

type WorkerRegistry struct {
	workers sync.Map // map[string]*WorkerInfo
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{}
}

func (r *WorkerRegistry) Register(id string, reg models.WorkerRegistration) {
	r.workers.Store(id, &WorkerInfo{
		ID:           id,
		Registration: reg,
		LastSeen:     time.Now(),
	})
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
			for _, cap := range info.Registration.Capabilities {
				if cap == capability || cap == "*" {
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
