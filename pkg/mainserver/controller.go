package mainserver

import (
	"net/http"
     "log"
	"github.com/gin-gonic/gin"
)

type MainController struct {
	service *MainService
}

func NewMainController(service *MainService) *MainController {
	return &MainController{service: service}
}

func (mc *MainController) Put(c *gin.Context) {

	var body map[string]string

	if err := c.BindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}

	if err := mc.service.Put(body); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "new version stored successfully"})
}

func (mc *MainController) Get(c *gin.Context) {
	key := c.Param("key")

	versions, err := mc.service.Get(key)
	
	if err != nil {
		log.Printf("[CONTROLLER] Error getting key='%s', err=%v", key, err)
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	if len(versions) == 0 {
		log.Printf("[CONTROLLER] No versions found for key='%s'", key)
		c.JSON(http.StatusNotFound, gin.H{"error": "key not found"})
		return
	}

	log.Printf("[CONTROLLER] Returning %d versions for key='%s'", len(versions), key)
	c.JSON(http.StatusOK, versions)
}

func (mc *MainController) GetPreferenceList(c *gin.Context) {
	key := c.Query("key")
	if key == "" {
		c.JSON(400, gin.H{"error": "key is required"})
		return
	}

	list, err := mc.service.GetPreferenceList(key)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, gin.H{"preferenceList": list})
}
