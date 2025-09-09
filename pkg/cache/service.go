package cache

type CacheService struct {
	repo *CacheRepository
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
