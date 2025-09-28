package xstorebackupbinlogs

import (
	domain_xstores "polardbx-ui-backend/pkg/api/domain/xstores"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes under /api/v1/crd/xstorebackupbinlogs
func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/xstorebackupbinlogs")
	r.GET("", domain_xstores.ListBackupBinlogs)
	r.POST("", domain_xstores.CreateBackupBinlog)
	item := r.Group("/:namespace/:name")
	item.GET("", domain_xstores.GetBackupBinlog)
	item.PUT("", domain_xstores.UpdateBackupBinlog)
	item.DELETE("", domain_xstores.DeleteBackupBinlog)
}
