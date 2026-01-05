package mainserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/rupeshx80/consistent-hashing/pkg/cache"
	"github.com/rupeshx80/consistent-hashing/pkg/hash-ring"
	"github.com/rupeshx80/consistent-hashing/pkg/quorum"
)

type VersionedValue struct {
	Value       string `json:"value"`
	VectorClock string `json:"vectorClock"`
	CreatedAt   string `json:"createdAt"`
}

type MainService struct {
	ring        *hashring.HashRing
	repository  *KeyValueRepository
	qManager    *quorum.QuorumManager
	cacheClient *cache.CacheClient
}

func NewMainService(ring *hashring.HashRing, repo *KeyValueRepository, qManager *quorum.QuorumManager, cacheClient *cache.CacheClient) *MainService {
	return &MainService{
		ring:        ring,
		repository:  repo,
		qManager:    qManager,
		cacheClient: cacheClient,
	}
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

// this persists locally (coordinator) and writes to replicas using the quorum manager.
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
	log.Printf("[DB] Key='%s' clientVC='%s'", key, clientVC)

	if err != nil {
		return fmt.Errorf("failed to build vector clock: %w", err)
	}

	log.Printf("[PUT] Key='%s' clientVC='%s' newVC='%s'", key, clientVC, newVC)

	//writes to cache first (fast path) -non-fatal if fails
	if s.cacheClient != nil {
		_ = s.cacheClient.WriteToCache(key, value, newVC)
	}

	//then we persist locally to DB (coordinator write)
	if err := s.repository.PutVersion(key, value, newVC); err != nil {
		return fmt.Errorf("failed to save new version: %w", err)
	}

	//Build replica list (exclude coordinator)
	preferenceList := s.ring.GetPreferenceList(key)
	replicas := make([]string, 0, len(preferenceList))
	for _, n := range preferenceList {
		if n == node {
			continue
		}
		replicas = append(replicas, n)
	}

	//Issue write quorum request to replicas and wait for quorum
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if len(replicas) > 0 {
		if err := s.qManager.WriteQuorum(ctx, replicas, key, value, newVC); err != nil {
			return fmt.Errorf("write quorum failed: %w", err)
		}
	}

	log.Printf("[PUT] SUCCESS - Key='%s' Coordinator=%s VC=%s", key, node, newVC)
	return nil
}


func (s *MainService) Get(key string) ([]VersionedValue, error) {
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	if s.cacheClient != nil {
		cacheVersions, err := s.cacheClient.ReadFromCache(key)
		if err == nil && len(cacheVersions) > 0 {
			log.Printf("[GET] Cache HIT for key='%s'", key)

			// Convert cache response to VersionedValue
			result := make([]VersionedValue, len(cacheVersions))
			for i, cv := range cacheVersions {
				result[i] = VersionedValue{
					Value:       cv.Value,
					VectorClock: cv.VectorClock,
					CreatedAt:   cv.CreatedAt,
				}
			}
			return result, nil
		}
		log.Printf("[GET] Cache MISS for key='%s', trying quorum/DB", key)
	}

	preferenceList := s.ring.GetPreferenceList(key)
	if len(preferenceList) == 0 {
		return nil, fmt.Errorf("no nodes available for key: %s", key)
	}

	//context with timeout for read quorum
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	qres, err := s.qManager.ReadQuorum(ctx, preferenceList, key)

	if err == nil && len(qres) > 0 {
		out := make([]VersionedValue, 0, len(qres))
		for _, v := range qres {
			out = append(out, VersionedValue{
				Value:       v.Value,
				VectorClock: v.VectorClock,
				CreatedAt:   v.CreatedAt,
			})
		}
		log.Printf("[GET] Quorum READ success for key='%s'", key)
		return out, nil
	}

	dbVersions, dbErr := s.repository.GetAllVersions(key)
	if dbErr != nil {
		return nil, fmt.Errorf("not found in cache, quorum, or DB: %w", dbErr)
	}

	var out []VersionedValue
	for _, kv := range dbVersions {
		out = append(out, VersionedValue{
			Value:       kv.Value,
			VectorClock: kv.VectorClock,
			CreatedAt:   kv.CreatedAt.String(),
		})
	}

	log.Printf("[GET] DB fallback for key='%s', found %d versions", key, len(dbVersions))
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

