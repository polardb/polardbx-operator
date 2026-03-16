package systemtasks

import (
	"github.com/gin-gonic/gin"
)

// RegisterRoutes unified registration entry for "platform task domain" (thin shell, forwarding to existing handlers).
func RegisterRoutes(v1 *gin.RouterGroup) {
	g := v1.Group("/systemtasks")
	g.GET("", List)
	g.POST("", Create)
	item := g.Group("/:namespace/:name")
	item.GET("", Get)
	item.PUT("", Update)
	item.DELETE("", Delete)
}
