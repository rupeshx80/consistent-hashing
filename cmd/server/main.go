package main

import (
	"log"
	"time"

	"github.com/rupeshx80/consistent-hashing/pkg/cache"
	"github.com/rupeshx80/consistent-hashing/pkg/db"
	"github.com/rupeshx80/consistent-hashing/pkg/hash-ring"
	"github.com/rupeshx80/consistent-hashing/pkg/mainserver"
	"github.com/rupeshx80/consistent-hashing/pkg/model"
	"github.com/rupeshx80/consistent-hashing/pkg/quorum"
)

func main() {
	// Connect to database
	db.Connect()

	// Note: fix this later, remove unique index
	err := db.RJ.Migrator().DropIndex(&model.KeyValue{}, "idx_key_values_key")
	if err != nil {
		log.Printf("Warning: Could not drop unique index: %v", err)
	}

	err = db.RJ.AutoMigrate(&model.KeyValue{})
	if err != nil {
		log.Fatal("Failed to migrate:", err)
	}

	log.Println("Database migrated successfully")

	// Initialize hash ring with 4 nodes
	ring := hashring.NewHashRing(3, 3)
	ring.AddNode(":6001", 1)
	ring.AddNode(":6002", 1)
	ring.AddNode(":6003", 1)
	ring.AddNode(":6004", 1)

	// Initialize repository and quorum manager
	repo := mainserver.NewKeyValueRepository()
	qConfig := quorum.NewQuorumConfig(3, 2, 2) // N=3, W=2, R=2 (follows Dynamo paper)
	qManager := quorum.NewQuorumManager(qConfig)

	// Start cache servers on each node
	go func() {
		log.Println("[CACHE-1] Cache server running on :6001")
		if err := cache.SetupRouter().Run(":6001"); err != nil {
			log.Fatalf("[CACHE-1] Failed to start: %v", err)
		}
	}()

	go func() {
		log.Println("[CACHE-2] Cache server running on :6002")
		if err := cache.SetupRouter().Run(":6002"); err != nil {
			log.Fatalf("[CACHE-2] Failed to start: %v", err)
		}
	}()

	go func() {
		log.Println("[CACHE-3] Cache server running on :6003")
		if err := cache.SetupRouter().Run(":6003"); err != nil {
			log.Fatalf("[CACHE-3] Failed to start: %v", err)
		}
	}()

	go func() {
		log.Println("[CACHE-4] Cache server running on :6004")
		if err := cache.SetupRouter().Run(":6004"); err != nil {
			log.Fatalf("[CACHE-4] Failed to start: %v", err)
		}
	}()

	//give cache servers time to start
	time.Sleep(2 * time.Second)

	//cache client pointing to first cache node
	//add load balancer 
	cacheClient := cache.NewCacheClient("http://127.0.0.1:6001")

	// Start main coordinator server
	log.Println("[MAIN] Main server running on :5000")
	if err := mainserver.SetupRouter(ring, repo, qManager, cacheClient).Run(":5000"); err != nil {
		log.Fatalf("[MAIN] Failed to start: %v", err)
	}
}