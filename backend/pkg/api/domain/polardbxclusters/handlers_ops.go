package polardbxclusters

import (
	"polardbx-ui-backend/pkg/api/domain/polardbxclusters/services"
	api_pod "polardbx-ui-backend/pkg/api/pod"
	api_prechange "polardbx-ui-backend/pkg/api/prechange"
	api_restore "polardbx-ui-backend/pkg/api/restore"

	"github.com/gin-gonic/gin"
)

// --- Thin handlers forwarding to services/others ---

func UpdateLogConfig(c *gin.Context) {
	_ = services.NewClusterService().UpdateLogConfig(c.Request.Context(), c)
}
func Scale(c *gin.Context)            { _ = services.NewClusterService().Scale(c.Request.Context(), c) }
func Upgrade(c *gin.Context)          { _ = services.NewClusterService().Upgrade(c.Request.Context(), c) }
func GetAlertsSummary(c *gin.Context) { services.GetAlertsSummary(c) }

func ListPods(c *gin.Context) { api_pod.ListForCluster(c) }

func GetPrechangeChecklist(c *gin.Context) { api_prechange.GetPrechangeChecklist(c) }
func Precheck(c *gin.Context)              { api_prechange.Precheck(c) }

func RestoreCluster(c *gin.Context)   { api_restore.RestoreCluster(c) }
func InitiatePITR(c *gin.Context)     { api_restore.InitiatePITR(c) }
func GetRestoreStatus(c *gin.Context) { api_restore.GetRestoreStatus(c) }
