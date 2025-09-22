package main

import (
	"log"

	"github.com/rupeshx80/consistent-hashing/pkg/cache"
	"github.com/rupeshx80/consistent-hashing/pkg/db"
	"github.com/rupeshx80/consistent-hashing/pkg/hash-ring"
	"github.com/rupeshx80/consistent-hashing/pkg/mainserver"
	"github.com/rupeshx80/consistent-hashing/pkg/model"
)

func main() {
	db.Connect()

	err := db.RJ.Migrator().DropIndex(&model.KeyValue{}, "idx_key_values_key")
	if err != nil {
		log.Printf("Warning: Could not drop unique index: %v", err)
	}
	err = db.RJ.AutoMigrate(&model.KeyValue{})
	if err != nil {
		log.Fatal("Failed to migrate:", err)
	}

	if err != nil {
		log.Fatal("Migration failed:", err)
	}
	log.Println("Database migrated")

	ring := hashring.NewHashRing(3, 3)
	ring.AddNode(":6001", 1)
	ring.AddNode(":6002", 1)
	ring.AddNode(":6003", 2)
	ring.AddNode(":6004", 1)

	repo := mainserver.NewKeyValueRepository()

	go func() {
		log.Println("Cache server 1 running on :6001")
		cache.SetupRouter().Run(":6001")
	}()
	go func() {
		log.Println("Cache server 2 running on :6002")
		cache.SetupRouter().Run(":6002")
	}()

	go func() {
		log.Println("Cache server 3 running on :6003")
		cache.SetupRouter().Run(":6003")
	}()
	go func() {
		log.Println("Cache server 4 running on :6004")
		cache.SetupRouter().Run(":6004")
	}()

	log.Println("Main server running on :5000")
	mainserver.SetupRouter(ring, repo).Run(":5000")

}
