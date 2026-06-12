package registry

import (
	"sync"

	"github.com/galgotech/heddle-lang/internal/models"
)

const numResultShards = 32

type resultShard struct {
	mu sync.RWMutex
	m  map[string]*models.TaskFuture
}

type shardedResultMap struct {
	shards [numResultShards]*resultShard
}

func newShardedResultMap() *shardedResultMap {
	sm := &shardedResultMap{}
	for i := range numResultShards {
		sm.shards[i] = &resultShard{
			m: make(map[string]*models.TaskFuture),
		}
	}
	return sm
}

func (sm *shardedResultMap) getShard(key string) *resultShard {
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return sm.shards[hash&(numResultShards-1)]
}

func (sm *shardedResultMap) set(key string, future *models.TaskFuture) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	shard.m[key] = future
	shard.mu.Unlock()
}

func (sm *shardedResultMap) delete(key string) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	delete(shard.m, key)
	shard.mu.Unlock()
}

func (sm *shardedResultMap) get(key string) (*models.TaskFuture, bool) {
	shard := sm.getShard(key)
	shard.mu.RLock()
	future, ok := shard.m[key]
	shard.mu.RUnlock()
	return future, ok
}

func (sm *shardedResultMap) clearAndCloseAll(errResult models.TaskResult) {
	for i := range numResultShards {
		shard := sm.shards[i]
		shard.mu.Lock()
		for key, future := range shard.m {
			errResult.TaskID = key
			future.Resolve(errResult)
			future.Close()
			delete(shard.m, key)
		}
		shard.mu.Unlock()
	}
}
