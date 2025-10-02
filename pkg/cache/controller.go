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
	
	//deduplicate by vector clock - keep only the first occurrence of each vector clock
	seenVectorClocks := make(map[string]bool)
	var uniqueVersions []map[string]string
	
	for _, v := range versions {
		if !seenVectorClocks[v.VectorClock] {
			seenVectorClocks[v.VectorClock] = true
			uniqueVersions = append(uniqueVersions, map[string]string{
				"value":       v.Value,
				"vectorClock": v.VectorClock,
				"createdAt":   v.CreatedAt.Format("2006-01-02 15:04:05.999999999 -0700 MST"),
			})
		}
	}
	
	ctx.JSON(http.StatusOK, uniqueVersions)
	log.Printf("[CACHE-CONTROLLER] Returned %d unique versions (from %d total) for key='%s'", 
		len(uniqueVersions), len(versions), key)
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
