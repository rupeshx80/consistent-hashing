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
	ring *hashring.HashRing
}

func NewMainService(ring *hashring.HashRing) *MainService {
	return &MainService{ring: ring}
}

func (s *MainService) Set(body map[string]string) error {

	key := body["key"]

	if key == "" {
		return fmt.Errorf("key is required")
	}
	node := s.ring.GetNode(key)

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

	resp, err := http.Get("http://127.0.0.1" + node + "/get/" + key)
	if err != nil {
		return "", fmt.Errorf("failed to forward: %w", err)
	}

	log.Printf("SUCCESS - Key '%s' stored on server %s", key, node)

	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	
	return string(data), nil
}
