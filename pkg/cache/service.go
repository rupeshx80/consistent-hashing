package cache

import (
	"github.com/rupeshx80/consistent-hashing/pkg/mainserver"
	"github.com/rupeshx80/consistent-hashing/pkg/model"
)

type CacheService struct {
	repo *CacheRepository
	kvRepo *mainserver.KeyValueRepository
}

func NewCacheService(repo *CacheRepository) *CacheService {
	return &CacheService{repo: repo}
}

func (s *CacheService) SetKey(key, value string) {
	s.repo.Set(key, value)
}

func (s *CacheService) GetKey(key string) (string, bool) {
	return s.repo.Get(key)
}

func (s *CacheService) GetAllVersions(key string) ([]model.KeyValue, error) {
	return s.kvRepo.GetAllVersions(key)
}