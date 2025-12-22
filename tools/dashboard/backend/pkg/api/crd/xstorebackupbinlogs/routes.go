package xstorebackupbinlogs

import (
	domain_xstores "polardbx-dashboard-backend/pkg/api/domain/xstores"
	"polardbx-dashboard-backend/pkg/api/routerutil"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes under /api/v1/crd/xstorebackupbinlogs
func RegisterRoutes(crd *gin.RouterGroup) {
	base := "/xstorebackupbinlogs"
	routerutil.RegisterCRUDWithItemPattern(crd, base, base+"/:namespace/:name", routerutil.CRUDHandlers{
		List:   domain_xstores.ListBackupBinlogs,
		Create: domain_xstores.CreateBackupBinlog,
		Get:    domain_xstores.GetBackupBinlog,
		Update: domain_xstores.UpdateBackupBinlog,
		Delete: domain_xstores.DeleteBackupBinlog,
	})
}
