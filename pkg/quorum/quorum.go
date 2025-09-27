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

// --- Write ---
func (qm *QuorumManager) WriteQuorum(ctx context.Context, nodes []string, key, value, vc string) error {
	log.Printf("[WRITE] Starting write quorum for key='%s', value='%s', vc='%s'", key, value, vc)

	required := qm.config.W - 1
	log.Printf("[WRITE] Required successful writes=%d", required)
	if required < 0 {
		return fmt.Errorf("invalid W=%d", qm.config.W)
	}

	payload, _ := json.Marshal(map[string]string{
		"key":         key,
		"value":       value,
		"vectorClock": vc,
	})
	log.Printf("[WRITE] Payload prepared=%s", string(payload))

	responses := make(chan QuorumResponse, len(nodes))
	var wg sync.WaitGroup

	for _, node := range nodes {
		log.Printf("[WRITE] Sending write request to node=%s", node)
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			resp, err := qm.httpClient.Post("http://127.0.0.1"+n+"/set", "application/json", bytes.NewBuffer(payload))
			if err != nil {
				log.Printf("[WRITE] Error writing to node=%s, err=%v", n, err)
				responses <- QuorumResponse{Success: false, Error: err}
				return
			}
			if resp.StatusCode != http.StatusOK {
				log.Printf("[WRITE] Node=%s returned non-200=%d", n, resp.StatusCode)
				responses <- QuorumResponse{Success: false, Error: fmt.Errorf("status=%d", resp.StatusCode)}
				return
			}
			resp.Body.Close()
			log.Printf("[WRITE] Node=%s write success", n)
			responses <- QuorumResponse{Success: true}
		}(node)
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	success := 0
	deadline := time.Now().Add(qm.timeout)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[WRITE] Context cancelled, err=%v", ctx.Err())
			return ctx.Err()
		case r, ok := <-responses:
			if !ok {
				log.Printf("[WRITE] Response channel closed, total success=%d", success)
				if success >= required {
					log.Printf("[WRITE] Write quorum satisfied")
					return nil
				}
				return fmt.Errorf("write quorum failed")
			}
			if r.Success {
				success++
				log.Printf("[WRITE] Success count=%d", success)
				if success >= required {
					log.Printf("[WRITE] Quorum satisfied, returning success")
					return nil
				}
			} else {
				log.Printf("[WRITE] Failed response received, err=%v", r.Error)
			}
		default:
			if time.Now().After(deadline) {
				log.Printf("[WRITE] Timeout reached, success=%d required=%d", success, required)
				if success >= required {
					return nil
				}
				return fmt.Errorf("write quorum timeout")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// --- Read ---
type VersionedValue struct {
	Value       string `json:"value"`
	VectorClock string `json:"vectorClock"`
	CreatedAt   string `json:"createdAt"`
}

func (qm *QuorumManager) ReadQuorum(ctx context.Context, nodes []string, key string) ([]VersionedValue, error) {
	log.Printf("[READ] Starting read quorum for key='%s'", key)

	responses := make(chan QuorumResponse, len(nodes))
	var wg sync.WaitGroup

	for _, node := range nodes {
		log.Printf("[READ] Sending read request to node=%s", node)
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			resp, err := qm.httpClient.Get("http://127.0.0.1" + n + "/get/" + key)
			if err != nil {
				log.Printf("[READ] Error reading from node=%s, err=%v", n, err)
				responses <- QuorumResponse{Success: false, Error: err}
				return
			}
			if resp.StatusCode != http.StatusOK {
				log.Printf("[READ] Node=%s returned non-200=%d", n, resp.StatusCode)
				responses <- QuorumResponse{Success: false, Error: fmt.Errorf("status=%d", resp.StatusCode)}
				return
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			log.Printf("[READ] Node=%s response=%s", n, string(data))

			var versions []VersionedValue
			if jsonErr := json.Unmarshal(data, &versions); jsonErr == nil {
				log.Printf("[READ] Node=%s parsed %d versions", n, len(versions))
				responses <- QuorumResponse{Success: true, Data: versions}
			} else {
				log.Printf("[READ] Node=%s failed JSON parse, err=%v", n, jsonErr)
				responses <- QuorumResponse{Success: false, Error: jsonErr}
			}
		}(node)
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	var results []VersionedValue
	success := 0
	deadline := time.Now().Add(qm.timeout)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[READ] Context cancelled, err=%v", ctx.Err())
			return nil, ctx.Err()
		case r, ok := <-responses:
			if !ok {
				log.Printf("[READ] Response channel closed, total success=%d", success)
				if success >= qm.config.R {
					log.Printf("[READ] Read quorum satisfied, returning results=%d", len(results))
					return results, nil
				}
				return nil, fmt.Errorf("read quorum failed")
			}
			if r.Success {
				if v, ok := r.Data.([]VersionedValue); ok {
					log.Printf("[READ] Adding %d versions to results", len(v))
					results = append(results, v...)
				}
				success++
				log.Printf("[READ] Success count=%d", success)
				if success >= qm.config.R {
					log.Printf("[READ] Quorum satisfied, returning results=%d", len(results))
					return results, nil
				}
			} else {
				log.Printf("[READ] Failed response received, err=%v", r.Error)
			}
		default:
			if time.Now().After(deadline) {
				log.Printf("[READ] Timeout reached, success=%d required=%d", success, qm.config.R)
				if success >= qm.config.R {
					return results, nil
				}
				return nil, fmt.Errorf("read quorum timeout")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
