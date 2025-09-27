package mainserver

import (

	"github.com/gin-gonic/gin"
	"github.com/rupeshx80/consistent-hashing/pkg/hash-ring"
	// "github.com/rupeshx80/consistent-hashing/pkg/quorum"
)

func SetupRouter(ring *hashring.HashRing, repo *KeyValueRepository) *gin.Engine {
	r := gin.Default()
	// service:= NewMainService(ring, repo,qManager), add later and remove below service
	service:= NewMainService(ring, repo)
	ctrl := NewMainController(service)

	r.PUT("/set", ctrl.Put)
	r.GET("/get/:key", ctrl.Get)
	r.GET("/preference-list", ctrl.GetPreferenceList)
	return r
}
