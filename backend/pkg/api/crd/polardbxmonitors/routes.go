package polardbxmonitors

import (
	api_monitor "polardbx-ui-backend/pkg/api/monitor"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/polardbxmonitors")
	r.GET("", api_monitor.List)
	r.POST("", api_monitor.Create)
	item := r.Group("/:namespace/:name")
	item.GET("", api_monitor.Get)
	item.PUT("", api_monitor.Update)
	item.DELETE("", api_monitor.Delete)
}
