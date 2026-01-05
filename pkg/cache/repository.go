package cache

import (
	"sync"
	"time"
)

type VersionedValue struct {
	Value       string
	VectorClock string
	CreatedAt   time.Time
}

type CacheRepository struct {
	data map[string][]VersionedValue
	mu   sync.RWMutex
}

func NewCacheRepository() *CacheRepository {
	return &CacheRepository{data: make(map[string][]VersionedValue)}
}

func (r *CacheRepository) Set(key, value string,vectorClock string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	newVersion := VersionedValue{
		Value:       value,
		VectorClock: vectorClock,
		CreatedAt:   time.Now(),
	}
	
	r.data[key] = append(r.data[key], newVersion)
}

func (r *CacheRepository) GetAllVersions(key string) ([]VersionedValue, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	versions, ok := r.data[key]

	if !ok {
		return nil, false
	}
	
	//copy of the slice to prevent external mutation of the internal data
	result := make([]VersionedValue, len(versions))
	copy(result, versions)
	return result, true
}

func (r *CacheRepository) Delete(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.data, key)
}

