package systemtasks

import (
	api_systemtask "polardbx-ui-backend/pkg/api/systemtask"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes adds /crd/systemtasks CRUD aliases.
func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/systemtasks")
	r.GET("", api_systemtask.List)
	r.POST("", api_systemtask.Create)
	item := r.Group("/:namespace/:name")
	item.GET("", api_systemtask.Get)
	item.PUT("", api_systemtask.Update)
	item.DELETE("", api_systemtask.Delete)
}
