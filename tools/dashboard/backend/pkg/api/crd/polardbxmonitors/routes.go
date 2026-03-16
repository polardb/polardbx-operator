package polardbxmonitors

import (
	domain_monitoring "polardbx-dashboard-backend/pkg/api/domain/monitoring"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/polardbxmonitors")
	r.GET("", domain_monitoring.ListMonitors)
	r.POST("", domain_monitoring.CreateMonitor)
	item := r.Group("/:namespace/:name")
	item.GET("", domain_monitoring.GetMonitor)
	item.PUT("", domain_monitoring.UpdateMonitor)
	item.DELETE("", domain_monitoring.DeleteMonitor)
}
