package systemtasks

import (
	"github.com/gin-gonic/gin"
)

// RegisterRoutes 统一注册“平台任务域”的入口（薄壳，转发到既有 handlers）。
func RegisterRoutes(v1 *gin.RouterGroup) {
	g := v1.Group("/systemtasks")
	g.GET("", List)
	g.POST("", Create)
	item := g.Group("/:namespace/:name")
	item.GET("", Get)
	item.PUT("", Update)
	item.DELETE("", Delete)
}
