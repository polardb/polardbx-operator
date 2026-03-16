package polardbxbackups

import (
	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes adds /crd/polardbxbackups aliases for non-cluster-scoped endpoints.
func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/polardbxbackups")
	// validation and overview/metrics
	r.POST("/validate", domain_pxc.ValidateBackup)
	r.GET("/overview", domain_pxc.GetBackupOverview)
	r.GET("/binlog/metrics", domain_pxc.GetBinlogMetrics)
	// item-scoped operations
	item := r.Group("/:namespace/:name")
	item.GET("/stream", domain_pxc.StreamBackupEvents)
	item.GET("/metrics", domain_pxc.GetBackupMetrics)
	item.DELETE("", domain_pxc.DeleteBackup)
}
