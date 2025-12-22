package polardbxbackupbinlogs

import (
	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/polardbxbackupbinlogs")
	r.GET("", domain_pxc.ListBackupBinlogs)
	r.POST("", domain_pxc.CreateBackupBinlog)
	item := r.Group("/:namespace/:name")
	item.GET("", domain_pxc.GetBackupBinlog)
	item.PUT("", domain_pxc.UpdateBackupBinlog)
	item.DELETE("", domain_pxc.DeleteBackupBinlog)
}
