package polardbxclusters

import (
	domain_pod "polardbx-dashboard-backend/pkg/api/domain/platform/pod/handler"
	domain_restore "polardbx-dashboard-backend/pkg/api/domain/platform/restore/handler"

	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"

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
	item.GET("/pods", domain_pod.ListForCluster)

	// Cluster backups (list/create) and advice → domain
	item.GET("/backups", domain_pxc.ListBackups)
	item.POST("/backups", domain_pxc.CreateBackup)
	item.GET("/backup-advice", domain_pxc.GetBackupAdvice)

	// Prechange & Restore
	item.GET("/prechange-check", domain_pxc.GetPrechangeChecklist)
	item.POST("/precheck", domain_pxc.Precheck)
	item.POST("/restore", domain_restore.RestoreCluster)
	item.POST("/pitr", domain_restore.InitiatePITR)
	item.GET("/restore-status", domain_restore.GetRestoreStatus)
}
