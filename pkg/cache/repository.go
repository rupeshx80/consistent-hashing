package cache

import "sync"

type CacheRepository struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewCacheRepository() *CacheRepository {
	return &CacheRepository{data: make(map[string]string)}
}

func (r *CacheRepository) Set(key, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[key] = value
}

func (r *CacheRepository) Get(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	val, ok := r.data[key]
	return val, ok
}
