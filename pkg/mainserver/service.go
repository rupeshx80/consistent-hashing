package mainserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/rupeshx80/consistent-hashing/pkg/hash-ring"
)

type MainService struct {
	ring       *hashring.HashRing
	repository *KeyValueRepository
}

func NewMainService(ring *hashring.HashRing, repo *KeyValueRepository) *MainService {
	return &MainService{ring: ring, repository: repo}
}

func (s *MainService) Set(body map[string]string) error {

	key := body["key"]
	value := body["value"]

	if key == "" {
		return fmt.Errorf("key is required")
	}

	if err := s.repository.UpsertKeyValue(key, value); err != nil {
		return fmt.Errorf("failed to save to DB: %w", err)
	}

	log.Printf("[DB] Key='%s' persisted with Value='%s'", key, value)

	node := s.ring.GetNode(key)
	log.Printf("[CONSISTENT-HASH] Key='%s' mapped to Cache Node='%s'", key, node)

	b, err := json.Marshal(body)

	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	resp, err := http.Post("http://127.0.0.1"+node+"/set", "application/json", bytes.NewBuffer(b))

	if err != nil {
		return fmt.Errorf("failed to forward: %w", err)
	}

	log.Printf("SET - Key: '%s' -> Cache Server: %s", key, node)

	defer resp.Body.Close()
	return nil
}

func (s *MainService) Get(key string) (string, error) {

	if key == "" {
		return "", fmt.Errorf("key is required")
	}

	node := s.ring.GetNode(key)
    log.Printf("[CONSISTENT-HASH] Looking up Key='%s' -> Node='%s'", key, node)

	resp, err := http.Get("http://127.0.0.1" + node + "/get/" + key)
	if err == nil && resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		log.Printf("CACHE HIT - Key '%s' from server %s", key, node)
		return string(data), nil
	}

	log.Printf("[CACHE-MISS] Key='%s' not found in Cache Node='%s'", key, node)

	kv, dbErr := s.repository.GetKeyValue(key)
	if dbErr != nil {
		log.Printf("[DB-MISS] Key='%s' not found in DB", key)
		return "", fmt.Errorf("not found in cache or DB: %w", dbErr)
	}

	log.Printf("[DB-HIT] Key='%s' found in DB with Value='%s'", key, kv.Value)

	body := map[string]string{"key": kv.Key, "value": string(kv.Value)}
	b, _ := json.Marshal(body)
	_, cacheErr := http.Post("http://127.0.0.1"+node+"/set", "application/json", bytes.NewBuffer(b))
	if cacheErr != nil {
		log.Printf("[CACHE-REPOPULATE-FAIL] Key='%s' -> Node='%s' | Error=%v", key, node, cacheErr)
	} else {
		log.Printf("[CACHE-REPOPULATE] Key='%s' synced back into Cache Node='%s'", key, node)
	}

	log.Printf("DB HIT - Key '%s' loaded from DB and synced to cache", key)
	return string(kv.Value), nil
}
