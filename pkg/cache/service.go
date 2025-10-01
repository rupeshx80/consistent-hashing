package cache

import (
	"errors"
	"log"
)

type CacheService struct {
	repo *CacheRepository
}

func NewCacheService(repo *CacheRepository) *CacheService {
	return &CacheService{
		repo: repo,
	}
}

func (s *CacheService) SetKey(key, value, vectorClock string) {
	s.repo.Set(key, value, vectorClock)
	log.Printf("[CACHE-SERVICE] Stored key='%s' value='%s' vc='%s'", key, value, vectorClock)
}

func (s *CacheService) GetAllVersions(key string) ([]VersionedValue, error) {
	versions, ok := s.repo.GetAllVersions(key)
	
	if !ok || len(versions) == 0 {
		log.Printf("[CACHE-SERVICE] Key not found in cache: '%s'", key)
		return nil, errors.New("key not found in cache")
	}
	
	log.Printf("[CACHE-SERVICE] Found %d versions for key='%s'", len(versions), key)
	return versions, nil
}

func (s *CacheService) DeleteKey(key string) {
	s.repo.Delete(key)
	log.Printf("[CACHE-SERVICE] Deleted key='%s'", key)
}