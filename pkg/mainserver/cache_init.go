package mainserver

import "log"

func InitializeCache(service *MainService) {
	keys, err := service.repository.GetAllKeys()
	if err != nil {
		log.Printf("[CACHE] Failed to fetch keys for cache rehydration: %v", err)
		return
	}

	count := 0
	for _, key := range keys {
		versions, err := service.repository.GetAllVersions(key)
		if err != nil {
			continue
		}
		for _, v := range versions {
			err := service.cacheClient.WriteToCache(key, v.Value, v.VectorClock)
			if err != nil {
				log.Printf("[CACHE] Failed to write key='%s' to cache: %v", key, err)
				continue
			}
			count++
		}
	}

	log.Printf("[CACHE] Rehydrated %d versions into cache", count)
}
