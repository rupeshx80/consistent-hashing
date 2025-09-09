package main

import (
	"github.com/rupeshx80/consistent-hashing/pkg/cache"
	"github.com/rupeshx80/consistent-hashing/pkg/db"
	"github.com/rupeshx80/consistent-hashing/pkg/mainserver"
	"log"
)

func main() {
	// Connect DB (main server only)
	db.Connect()

	// Start 3 cache servers concurrently
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

	// Start main server (blocking)
	log.Println("Main server running on :5000")
	mainserver.SetupRouter().Run(":5000")
}
