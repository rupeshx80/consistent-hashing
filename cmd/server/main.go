package main

import (
	"github.com/rupeshx80/consistent-hashing/pkg/cache"
	"github.com/rupeshx80/consistent-hashing/pkg/db"
	"github.com/rupeshx80/consistent-hashing/pkg/mainserver"
	"log"
)

func main() {
	db.Connect()

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

	log.Println("Main server running on :5000")
	mainserver.SetupRouter().Run(":5000")
}
