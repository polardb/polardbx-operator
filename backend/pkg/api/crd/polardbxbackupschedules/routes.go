package polardbxbackupschedules

import (
	"github.com/gin-gonic/gin"

	domain_pxc "polardbx-ui-backend/pkg/api/domain/polardbxclusters"
)

func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/polardbxbackupschedules")
	r.GET("", domain_pxc.ListSchedules)
	r.POST("", domain_pxc.CreateSchedule)
	item := r.Group("/:namespace/:name")
	item.GET("", domain_pxc.GetSchedule)
	item.PUT("", domain_pxc.UpdateSchedule)
	item.DELETE("", domain_pxc.DeleteSchedule)
}
