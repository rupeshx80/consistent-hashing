package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

type CacheClient struct {
	baseURL string
}

func NewCacheClient(baseURL string) *CacheClient {
	return &CacheClient{
		baseURL: baseURL,
	}
}

func (c *CacheClient) WriteToCache(key, value, vectorClock string) error {

	if c.baseURL == "" {
		return nil 
	}

	payload := map[string]string{	
		"key":         key,
		"value":       value,
		"vectorClock": vectorClock,
	}

	jsonData, err := json.Marshal(payload)

	if err != nil {
		return fmt.Errorf("failed to marshal cache payload: %w", err)
	}

	resp, err := http.Post(c.baseURL+"/set", "application/json", bytes.NewBuffer(jsonData))

	if err != nil {
		log.Printf("[CACHE-CLIENT] Warning: failed to write to cache: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("[CACHE-CLIENT] Warning: cache returned status %d, body: %s", resp.StatusCode, string(body))
		return fmt.Errorf("cache write failed with status %d", resp.StatusCode)
	}

	log.Printf("[CACHE-CLIENT] Successfully wrote key='%s' to cache", key)
	return nil
}

func (c *CacheClient) ReadFromCache(key string) ([]CacheVersionedValue, error) {

	if c.baseURL == "" {
		return nil, fmt.Errorf("cache not configured")
	}

	resp, err := http.Get(c.baseURL + "/get/" + key)

	if err != nil {
		return nil, fmt.Errorf("cache request failed: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cache miss: status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)

	if err != nil {
		return nil, fmt.Errorf("failed to read cache response: %w", err)
	}

	var versions []CacheVersionedValue  //single key can have multiple versions, thats why we used in here
	if err := json.Unmarshal(data, &versions); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cache response: %w", err)
	}

	log.Printf("[CACHE-CLIENT] Cache HIT for key='%s', found %d versions", key, len(versions))
	return versions, nil
}

type CacheVersionedValue struct {
	Value       string `json:"value"`
	VectorClock string `json:"vectorClock"`
	CreatedAt   string `json:"createdAt"`
}

