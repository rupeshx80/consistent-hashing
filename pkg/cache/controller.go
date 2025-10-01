package cache

import (
	"net/http"
	"log"
	"github.com/gin-gonic/gin"
)

type CacheController struct {
	service *CacheService
}

type KeyVal struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	VectorClock string `json:"vectorClock"`
}

func NewCacheController(service *CacheService) *CacheController {
	return &CacheController{
		service: service,
	}
}

func (cc *CacheController) Set(ctx *gin.Context) {
	var req KeyVal

	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	if req.Key == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}
	
	cc.service.SetKey(req.Key, req.Value, req.VectorClock)

	log.Printf("[CACHE-CONTROLLER] Stored key='%s' value='%s' vc='%s'", req.Key, req.Value, req.VectorClock)
	ctx.JSON(http.StatusOK, gin.H{"message": "stored successfully"})
}

func (cc *CacheController) Get(ctx *gin.Context) {
	key := ctx.Param("key")

	if key == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}

	versions, err := cc.service.GetAllVersions(key)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "key not found"})
		return
	}

	if len(versions) == 0 {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "key not found"})
		return
	}
	
	// Convert to response format matching mainserver's VersionedValue
	response := make([]map[string]string, len(versions))
	
	for i, v := range versions {
		response[i] = map[string]string{
			"value":       v.Value,
			"vectorClock": v.VectorClock,
			"createdAt":   v.CreatedAt.Format("2006-01-02 15:04:05.999999999 -0700 MST"),
		}
	}
	
	ctx.JSON(http.StatusOK, response)
	log.Printf("[CACHE-CONTROLLER] Returned %d versions for key='%s'", len(versions), key)
}

func (cc *CacheController) Delete(ctx *gin.Context) {
	key := ctx.Param("key")

	if key == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}

	cc.service.DeleteKey(key)
	ctx.JSON(http.StatusOK, gin.H{"message": "deleted successfully"})
}
