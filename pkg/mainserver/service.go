package mainserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/rupeshx80/consistent-hashing/pkg/hash-ring"
)

// VersionedValue returned to clients
type VersionedValue struct {
	Value       string `json:"value"`
	VectorClock string `json:"vectorClock"`
	CreatedAt   string `json:"createdAt"`
}

type MainService struct {
	ring       *hashring.HashRing
	repository *KeyValueRepository
}

func NewMainService(ring *hashring.HashRing, repo *KeyValueRepository) *MainService {
	return &MainService{ring, repo}
}

// mergeMapMax merges integer counters, taking max per node
func mergeMapMax(dst, src map[string]int) {
	for k, v := range src {
		if cur, ok := dst[k]; !ok || v > cur {
			dst[k] = v
		}
	}
}

func parseVC(vc string) map[string]int {
	out := map[string]int{}

	if vc == "" {
		return out
	}

	_ = json.Unmarshal([]byte(vc), &out) 

	return out
}

func serializeVC(vc map[string]int) string {
	b, _ := json.Marshal(vc)
	return string(b)
}

func (s *MainService) buildNewVectorClock(key string, nodeID string, clientVC string) (string, error) {
	merged := map[string]int{}

	nodeID = strings.TrimSpace(nodeID)
    nodeID = strings.TrimPrefix(nodeID, ":")

	dbVersions, err := s.repository.GetAllVersions(key)

	if err == nil {
		for _, kv := range dbVersions {
			vcMap := parseVC(kv.VectorClock)
			mergeMapMax(merged, vcMap)
		}
	}

	if clientVC != "" {
		//here all dbs vector clock merges with client vc
		mergeMapMax(merged, parseVC(clientVC))
	}
    
	//nodes counter increment
	if _, ok := merged[nodeID]; !ok {
		merged[nodeID] = 1
	} else {
		merged[nodeID] = merged[nodeID] + 1
	}

	return serializeVC(merged), nil
}


func (s *MainService) Put(body map[string]string) error {
	key := body["key"]
	value := body["value"]
	clientVC := body["vectorClock"]

	if key == "" {
		return fmt.Errorf("key is required")
	}

	_, node := s.ring.GetNode(key)
	nodeID := node //use node string as node identifier for VC counters

	newVC, err := s.buildNewVectorClock(key, nodeID, clientVC)

	log.Printf("[DB] Key='%s' Whats the clientVC='%s'",key, clientVC)


	if err != nil {
		return fmt.Errorf("failed to build vector clock: %w", err)
	}

	if err := s.repository.PutVersion(key, value, newVC); err != nil {
		return fmt.Errorf("failed to save new version: %w", err)
	}

	log.Printf("[DB] Key='%s' new version persisted with VC='%s'", key, newVC)

	replicaBody := map[string]string{
		"key":         key,
		"value":       value,
		"vectorClock": newVC,
	}

	b, _ := json.Marshal(replicaBody)

	preferenceList := s.ring.GetPreferenceList(key)
	
	for _, n := range preferenceList {
		go func(n string) {
			resp, err := http.Post("http://127.0.0.1"+n+"/set", "application/json", bytes.NewBuffer(b))
			if err != nil {
				log.Printf("[REPLICATION-FAIL] Key='%s' -> Node='%s' | Error=%v", key, n, err)
				return 
			}
			resp.Body.Close()
			log.Printf("[REPLICATED] Key='%s' -> Node='%s'", key, n)
		}(n)
	}

	log.Printf("PUT - Key: '%s' -> Coordinator Node: %s", key, node)
	return nil
}


func (s *MainService) Get(key string) ([]VersionedValue, error) {
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	_, node := s.ring.GetNode(key)
	log.Printf("[CONSISTENT-HASH] Looking up Key='%s' -> Real Node='%s'", key, node)

	resp, err := http.Get("http://127.0.0.1" + node + "/get/" + key)
	if err == nil && resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err == nil {
			var versions []VersionedValue
			if err := json.Unmarshal(data, &versions); err == nil {
				log.Printf("[CACHE-HIT] Key='%s' from cache node %s", key, node)
				return versions, nil
			}
			log.Printf("[CACHE-ERROR] Failed to parse JSON from %s: %v", node, err)
		} else {
			log.Printf("[CACHE-ERROR] Failed to read body from %s: %v", node, err)
		}
	}
	log.Printf("[CACHE-MISS] Key='%s' not found in cache node %s", key, node)

	dbVersions, dbErr := s.repository.GetAllVersions(key)
	if dbErr != nil {
		log.Printf("[DB-MISS] Key='%s' not found in DB", key)
		return nil, fmt.Errorf("not found in cache or DB: %w", dbErr)
	}

	var out []VersionedValue
	for _, kv := range dbVersions {
		out = append(out, VersionedValue{
			Value:       kv.Value,
			VectorClock: kv.VectorClock,
			CreatedAt:   kv.CreatedAt.String(),
		})
	}

	go func() {
		b, _ := json.Marshal(out)
		_, cacheErr := http.Post("http://127.0.0.1"+node+"/set", "application/json", bytes.NewBuffer(b))
		if cacheErr != nil {
			log.Printf("[CACHE-REPOPULATE-FAIL] Key='%s' -> Node='%s' | Error=%v", key, node, cacheErr)
		} else {
			log.Printf("[CACHE-REPOPULATE] Key='%s' synced to Cache Node='%s'", key, node)
		}
	}()

	return out, nil
}


func (s *MainService) GetPreferenceList(key string) ([]string, error) {
    
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	preferenceList := s.ring.GetPreferenceList(key)

	if len(preferenceList) == 0 {
		return nil, fmt.Errorf("no nodes available for key: %s", key)
	}

	log.Printf("[CONSISTENT-HASH] Preference List for Key='%s': %v", key, preferenceList)
	return preferenceList, nil
}
