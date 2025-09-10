package mainserver

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type MainController struct {
	service *MainService
}

func NewMainController(service *MainService) *MainController {
	return &MainController{service: service}
}

func (mc *MainController) Set(c *gin.Context) {

	var body map[string]string

	if err := c.BindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}

	if err := mc.service.Set(body); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "stored successfully"})
}

func (mc *MainController) Get(c *gin.Context) {
	key := c.Param("key")

	value, err := mc.service.Get(key)
	
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"value": value})
}
