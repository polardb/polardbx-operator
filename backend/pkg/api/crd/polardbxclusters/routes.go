package polardbxclusters

import (
	api_pod "polardbx-ui-backend/pkg/api/pod"
	api_prechange "polardbx-ui-backend/pkg/api/prechange"
	api_restore "polardbx-ui-backend/pkg/api/restore"

	domain_pxc "polardbx-ui-backend/pkg/api/domain/polardbxclusters"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes adds /crd/polardbxclusters CRUD aliases and cluster-scoped subroutes.
func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/polardbxclusters")
	// CRUD → domain
	r.GET("", domain_pxc.List)
	r.POST("", domain_pxc.Create)
	item := r.Group("/:namespace/:name")
	item.GET("", domain_pxc.Get)
	item.PUT("", domain_pxc.Update)
	item.DELETE("", domain_pxc.Delete)

	// Cluster ops
	item.PATCH("/log-config/:nodeType", domain_pxc.UpdateLogConfig)
	item.PATCH("/scale", domain_pxc.Scale)
	item.PATCH("/upgrade", domain_pxc.Upgrade)
	item.GET("/alerts-summary", domain_pxc.GetAlertsSummary)

	// Cluster pods
	item.GET("/pods", api_pod.ListForCluster)

	// Cluster backups (list/create) and advice → domain
	item.GET("/backups", domain_pxc.ListBackups)
	item.POST("/backups", domain_pxc.CreateBackup)
	item.GET("/backup-advice", domain_pxc.GetBackupAdvice)

	// Prechange & Restore
	item.GET("/prechange-check", api_prechange.GetPrechangeChecklist)
	item.POST("/precheck", api_prechange.Precheck)
	item.POST("/restore", api_restore.RestoreCluster)
	item.POST("/pitr", api_restore.InitiatePITR)
	item.GET("/restore-status", api_restore.GetRestoreStatus)
}
