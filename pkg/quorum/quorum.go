package quorum

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type QuorumConfig struct {
	N int 
	R int 
	W int 
}

func NewQuorumConfig(n, r, w int) *QuorumConfig {
	return &QuorumConfig{N: n, R: r, W: w}
}

type QuorumResponse struct {
	Success bool
	Data    interface{}
	Error   error
	NodeID  string
}

type QuorumManager struct {
	config     *QuorumConfig
	httpClient *http.Client
	timeout    time.Duration
}

func NewQuorumManager(config *QuorumConfig) *QuorumManager {
	return &QuorumManager{
		config:     config,
		httpClient: &http.Client{Timeout: 2 * time.Second},
		timeout:    5 * time.Second,
	}
}

func (qm *QuorumManager) WriteQuorum(ctx context.Context, nodes []string, key, value, vc string) error {
	log.Printf("[WRITE] Starting write quorum for key='%s', value='%s', vc='%s'", key, value, vc)

	// We need W-1 successful replica writes (coordinator already wrote locally)
	required := qm.config.W - 1
	log.Printf("[WRITE] Required successful replica writes=%d", required)

	if required < 0 {
		return fmt.Errorf("invalid W=%d", qm.config.W)
	}

	// If no replicas needed, return success
	if required == 0 {
		log.Printf("[WRITE] No replica writes required")
		return nil
	}

	payload, err := json.Marshal(map[string]string{
		"key":         key,
		"value":       value,
		"vectorClock": vc,
	})

	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	log.Printf("[WRITE] Payload prepared=%s", string(payload))

	responses := make(chan QuorumResponse, len(nodes))
	var wg sync.WaitGroup

	// Send requests to all replica nodes
	for _, node := range nodes {
		log.Printf("[WRITE] Sending write request to replica node=%s", node)
		wg.Add(1)

		go func(n string) {
			defer wg.Done()

			resp, err := qm.httpClient.Post("http://127.0.0.1"+n+"/set", "application/json", bytes.NewBuffer(payload))

			if err != nil {
				log.Printf("[WRITE] Error writing to replica node=%s, err=%v", n, err)
				responses <- QuorumResponse{Success: false, Error: err, NodeID: n}
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf("[WRITE] Replica node=%s returned non-200=%d", n, resp.StatusCode)
				responses <- QuorumResponse{Success: false, Error: fmt.Errorf("status=%d", resp.StatusCode), NodeID: n}
				return
			}

			log.Printf("[WRITE] Replica node=%s write success", n)
			responses <- QuorumResponse{Success: true, NodeID: n}
		}(node)
	}

	// Close responses channel when all goroutines complete
	go func() {
		wg.Wait()
		close(responses)
	}()

	successCount := 0
	failedNodes := make([]string, 0)
	deadline := time.Now().Add(qm.timeout)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[WRITE] Context cancelled, err=%v", ctx.Err())
			return ctx.Err()

		case r, ok := <-responses:
			if !ok {
				// All responses received
				log.Printf("[WRITE] All responses received, success=%d, required=%d", successCount, required)
				if successCount >= required {
					log.Printf("[WRITE] Write quorum satisfied")
					return nil
				}
				return fmt.Errorf("write quorum failed: got %d successes, needed %d (failed nodes: %v)",
					successCount, required, failedNodes)
			}
            
			//you get low latency by returning as soon as quorum is met.
			if r.Success {
				successCount++
				log.Printf("[WRITE] Success from node=%s, total success=%d", r.NodeID, successCount)
				if successCount >= required {
					log.Printf("[WRITE] Write quorum satisfied early")
					return nil
				}
			} else {
				failedNodes = append(failedNodes, r.NodeID)
				log.Printf("[WRITE] Failed response from node=%s, err=%v", r.NodeID, r.Error)
			}
        
			//this runs when no ctx cancellation by client or server failure or no resp ready on channel.
		default:
			if time.Now().After(deadline) {
				log.Printf("[WRITE] Timeout reached, success=%d required=%d", successCount, required)
				if successCount >= required {
					return nil
				}
				return fmt.Errorf("write quorum timeout: got %d successes, needed %d (failed nodes: %v)",
					successCount, required, failedNodes)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

//represents a versioned value from storage
type VersionedValue struct {
	Value       string `json:"value"`
	VectorClock string `json:"vectorClock"`
	CreatedAt   string `json:"createdAt"`
	NodeID      string `json:"nodeId,omitempty"` //tracks which node returned this
}

func (qm *QuorumManager) ReadQuorum(ctx context.Context, nodes []string, key string) ([]VersionedValue, error) {
	log.Printf("[READ] Starting read quorum for key='%s', R=%d", key, qm.config.R)

	responses := make(chan QuorumResponse, len(nodes))
	var wg sync.WaitGroup

	// Send read requests to all nodes in preference list
	for _, node := range nodes {
		log.Printf("[READ] Sending read request to node=%s", node)
		
		wg.Add(1)
		go func(n string) {
			defer wg.Done()

			resp, err := qm.httpClient.Get("http://127.0.0.1" + n + "/get/" + key)
			if err != nil {
				log.Printf("[READ] Error reading from node=%s, err=%v", n, err)
				responses <- QuorumResponse{Success: false, Error: err, NodeID: n}
				return
			}
			defer resp.Body.Close() //for preventing http resource leak

			if resp.StatusCode != http.StatusOK {
				log.Printf("[READ] Node=%s returned non-200=%d", n, resp.StatusCode)
				responses <- QuorumResponse{Success: false, Error: fmt.Errorf("status=%d", resp.StatusCode), NodeID: n}
				return
			}

			data, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Printf("[READ] Error reading response body from node=%s, err=%v", n, err)
				responses <- QuorumResponse{Success: false, Error: err, NodeID: n}
				return
			}

			log.Printf("[READ] Node=%s response=%s", n, string(data))

			var versions []VersionedValue
			if jsonErr := json.Unmarshal(data, &versions); jsonErr != nil {
				log.Printf("[READ] Node=%s failed JSON parse, err=%v", n, jsonErr)
				responses <- QuorumResponse{Success: false, Error: jsonErr, NodeID: n}
				return
			}

			//versions with the node that returned them
			for i := range versions {
				versions[i].NodeID = n
			}

			log.Printf("[READ] Node=%s parsed %d versions", n, len(versions))
			responses <- QuorumResponse{Success: true, Data: versions, NodeID: n}
		}(node)
	}

	//closes responses channel when all goroutines complete
	go func() {
		wg.Wait()
		close(responses)
	}()

	var allVersions []VersionedValue
	successCount := 0
	failedNodes := make([]string, 0)
	deadline := time.Now().Add(qm.timeout)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[READ] Context cancelled, err=%v", ctx.Err())
			return nil, ctx.Err()

		case r, ok := <-responses:
			if !ok {
				// All responses received
				log.Printf("[READ] All responses received, success=%d, required=%d", successCount, qm.config.R)
				if successCount >= qm.config.R {
					log.Printf("[READ] Read quorum satisfied, returning %d versions", len(allVersions))
					return qm.deduplicateVersions(allVersions), nil
				}
				return nil, fmt.Errorf("read quorum failed: got %d successes, needed %d (failed nodes: %v)",
					successCount, qm.config.R, failedNodes)
			}

			if r.Success {
				if versions, ok := r.Data.([]VersionedValue); ok {
					log.Printf("[READ] Adding %d versions from node=%s", len(versions), r.NodeID)
					allVersions = append(allVersions, versions...)
				}
				successCount++
				log.Printf("[READ] Success from node=%s, total success=%d", r.NodeID, successCount)

				if successCount >= qm.config.R {
					log.Printf("[READ] Read quorum satisfied, returning %d versions", len(allVersions))
					return qm.deduplicateVersions(allVersions), nil
				}
			} else {
				failedNodes = append(failedNodes, r.NodeID)
				log.Printf("[READ] Failed response from node=%s, err=%v", r.NodeID, r.Error)
			}

		default:
			if time.Now().After(deadline) {
				log.Printf("[READ] Timeout reached, success=%d required=%d", successCount, qm.config.R)
				if successCount >= qm.config.R {
					return qm.deduplicateVersions(allVersions), nil
				}
				return nil, fmt.Errorf("read quorum timeout: got %d successes, needed %d (failed nodes: %v)",
					successCount, qm.config.R, failedNodes)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

//this removes duplicate versions based on vector clock
func (qm *QuorumManager) deduplicateVersions(versions []VersionedValue) []VersionedValue {
	seen := make(map[string]VersionedValue)

	for _, v := range versions {
		key := v.Value + "|" + v.VectorClock + "|" + v.CreatedAt
		if existing, exists := seen[key]; !exists {
			seen[key] = v
		} else {
			// Keep the one with node info if available
			if existing.NodeID == "" && v.NodeID != "" {
				seen[key] = v
			}
		}
	}

	result := make([]VersionedValue, 0, len(seen))
	for _, v := range seen {
		result = append(result, v)
	}

	log.Printf("[READ] Deduplicated %d versions to %d unique versions", len(versions), len(result))
	return result
}
