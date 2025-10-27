package polardbxclusters

import (
	api_clusterknobs "polardbx-ui-backend/pkg/api/clusterknobs"
	"polardbx-ui-backend/pkg/api/domain/polardbxclusters/services"

	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes 统一注册“逻辑集群域”的入口（薄壳，转发到既有 handlers）。
func RegisterRoutes(v1 *gin.RouterGroup) {
	g := v1.Group("/polardbxclusters")

	// Clusters CRUD & Ops (via thin handlers in this package)
	g.GET("", List)
	g.POST("", Create)
	g.POST("/:namespace/create", CreateFromConfig)
	item := g.Group("/:namespace/:name")
	item.GET("", Get)
	item.PUT("", Update)
	item.DELETE("", Delete)
	item.PATCH("/log-config/:nodeType", UpdateLogConfig)
	item.PATCH("/scale", Scale)
	item.PATCH("/upgrade", Upgrade)
	item.GET("/alerts-summary", GetAlertsSummary)

	// Cluster pods
	item.GET("/pods", ListPods)

	// Cluster-scoped backup ops (via thin handlers)
	item.GET("/backups", ListBackups)
	item.POST("/backups", CreateBackup)
	item.GET("/backup-advice", GetBackupAdvice)

	// Prechange & Restore
	item.GET("/prechange-check", GetPrechangeChecklist)
	item.POST("/precheck", Precheck)
	item.POST("/restore", RestoreCluster)
	item.POST("/pitr", InitiatePITR)
	item.GET("/restore-status", GetRestoreStatus)

	// Optional flow endpoints gated by env (default off)
	if strings.EqualFold(os.Getenv("ENABLE_FLOW_ENDPOINTS"), "true") {
		item.POST("/backup-flow/run", services.RunBackupFlow)
		item.POST("/restore-flow/run", services.RunRestoreFlow)
	}

	// Non-cluster-scoped under this domain
	// BackupSchedules
	g.GET("/backup-schedules", ListSchedules)
	g.POST("/backup-schedules", CreateSchedule)
	g.GET("/backup-schedules/:namespace/:name", GetSchedule)
	g.PUT("/backup-schedules/:namespace/:name", UpdateSchedule)
	g.DELETE("/backup-schedules/:namespace/:name", DeleteSchedule)
	// next-run aggregation (keep same path semantics under /api/v1)
	v1.GET("/backup-schedules/next-run", GetScheduleNextRuns)

	// BackupBinlogs
	g.GET("/backup-binlogs", ListBackupBinlogs)
	g.POST("/backup-binlogs", CreateBackupBinlog)
	g.GET("/backup-binlogs/:namespace/:name", GetBackupBinlog)
	g.PUT("/backup-binlogs/:namespace/:name", UpdateBackupBinlog)
	g.DELETE("/backup-binlogs/:namespace/:name", DeleteBackupBinlog)

	// Root-level backup operations (validate/stream/metrics/delete)
	g.POST("/backups/validate", ValidateBackup)
	g.GET("/backups/:namespace/:name/stream", StreamBackupEvents)
	g.GET("/backups/:namespace/:name/metrics", GetBackupMetrics)
	g.DELETE("/backups/:namespace/:name", DeleteBackup)

	// Parameters
	g.GET("/parameters", ListParameters)
	g.POST("/parameters", CreateParameter)
	g.GET("/parameters/:name", GetParameter)
	g.PUT("/parameters/:name", UpdateParameter)
	g.DELETE("/parameters/:name", DeleteParameter)

	g.GET("/parameter-templates", ListTemplates)
	g.POST("/parameter-templates", CreateTemplate)
	g.GET("/parameter-templates/:namespace/:name", GetTemplate)
	g.PUT("/parameter-templates/:namespace/:name", UpdateTemplate)
	g.DELETE("/parameter-templates/:namespace/:name", DeleteTemplate)

	// ClusterKnobs
	g.GET("/cluster-knobs", api_clusterknobs.GetList)
	g.POST("/cluster-knobs", api_clusterknobs.Create)
	g.GET("/cluster-knobs/:namespace/:name", api_clusterknobs.Get)
	g.PUT("/cluster-knobs/:namespace/:name", api_clusterknobs.Update)
	g.DELETE("/cluster-knobs/:namespace/:name", api_clusterknobs.Delete)
}
