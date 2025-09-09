package cache

import "github.com/gin-gonic/gin"

func SetupRouter() *gin.Engine {
	r := gin.Default()

	repo := NewCacheRepository()
	svc := NewCacheService(repo)
	ctrl := NewCacheController(svc)

	r.POST("/set", ctrl.Set)
	r.GET("/get/:key", ctrl.Get)

	return r
}
