package mainserver

import (

	"github.com/gin-gonic/gin"
	"github.com/rupeshx80/consistent-hashing/pkg/hash-ring"
)

func SetupRouter(ring *hashring.HashRing) *gin.Engine {
	r := gin.Default()
	service:= NewMainService(ring)
	ctrl := NewMainController(service)

	r.POST("/set", ctrl.Set)
	r.GET("/get/:key", ctrl.Get)
	return r
}
