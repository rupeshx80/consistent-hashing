package main

import (
	"log"

	"github.com/gin-gonic/gin"
	"github.com/rupeshx80/consistent-hashing/pkg/db"
	// "github.com/rupeshx80/go-crud/pkg/router"
)

func main() {
    db.Connect()
	r := gin.Default()
	r.SetTrustedProxies(nil)
//    r := router.SetupRouter()

 r.GET("/", func(c *gin.Context) {
        c.JSON(200, gin.H{"message":"Consistent-hashing baby"})
    })

    log.Println("Server is running on port 5000")
	r.Run(":5000")
}