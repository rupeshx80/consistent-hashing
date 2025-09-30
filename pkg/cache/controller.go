package cache

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type CacheController struct {
	service *CacheService
}

type KeyVal struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewCacheController(service *CacheService) *CacheController {
	return &CacheController{service: service}
}

func (cc *CacheController) Set(ctx *gin.Context) {

	var req KeyVal

	err := ctx.ShouldBindJSON(&req)

	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cc.service.SetKey(req.Key, req.Value)
	ctx.JSON(http.StatusOK, gin.H{"message": "stored successfully"})
}

func (cc *CacheController) Get(ctx *gin.Context) {
	key := ctx.Param("key")

	versions, err := cc.service.GetAllVersions(key)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "key not found"})
		return
	}

	if len(versions) == 0 {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "key not found"})
		return
	}

	// Always return as an array of versions
	ctx.JSON(http.StatusOK, versions)
}

