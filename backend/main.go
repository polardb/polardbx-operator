package main

import (
	"log"
	"net/http"
	"polardbx-ui-backend/pkg/api"
	api_alerts "polardbx-ui-backend/pkg/api/alerts"
	api_auth "polardbx-ui-backend/pkg/api/auth"
	api_clusterknobs "polardbx-ui-backend/pkg/api/clusterknobs"
	api_diagnostics "polardbx-ui-backend/pkg/api/diagnostics"
	api_grafana "polardbx-ui-backend/pkg/api/grafana"
	api_logcollector "polardbx-ui-backend/pkg/api/logcollector"
	api_logs "polardbx-ui-backend/pkg/api/logs"
	api_logservice "polardbx-ui-backend/pkg/api/logservice"
	api_logstrategy "polardbx-ui-backend/pkg/api/logstrategy"
	api_monitor "polardbx-ui-backend/pkg/api/monitor"
	api_monitoring "polardbx-ui-backend/pkg/api/monitoring"
	api_pod "polardbx-ui-backend/pkg/api/pod"
	api_prometheusrule "polardbx-ui-backend/pkg/api/prometheusrule"
	api_restore "polardbx-ui-backend/pkg/api/restore"
	api_router "polardbx-ui-backend/pkg/api/router"
	api_settings "polardbx-ui-backend/pkg/api/settings"
	api_system "polardbx-ui-backend/pkg/api/system"

	// domain handlers
	domain_pxc "polardbx-ui-backend/pkg/api/domain/polardbxclusters"
	domain_st "polardbx-ui-backend/pkg/api/domain/systemtasks"
	domain_xs "polardbx-ui-backend/pkg/api/domain/xstores"

	// domain_xs removed: xstores routes are now provided via RegisterDomainRoutes

	"github.com/gin-gonic/gin"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// CORS middleware
func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With, X-Kubeconfig-B64")
		c.Header("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE, PATCH")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func main() {
	// 设置controller-runtime日志器
	logger := zap.New(zap.UseDevMode(true))
	ctrllog.SetLogger(logger)

	r := gin.Default()

	// CORS middleware
	r.Use(CORSMiddleware())

	// A dummy ping endpoint
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong"})
	})

	// API v1 group
	v1 := r.Group("/api/v1")

	// The connect endpoint is special, it establishes the client for subsequent requests
	v1.POST("/connect", api.Connect)

	// JWT login endpoints (optional). When JWT_SECRET is set, protect routes with JWT.
	v1.POST("/auth/login", api_auth.Login)
	v1.GET("/auth/me", api_auth.Me)

	// All other endpoints require a valid kubeconfig
	v1.Use(api.KubeconfigAuthMiddleware())
	// Optional JWT middleware
	v1.Use(api_auth.JWTAuthMiddleware())
	{
		// Cluster routes → domain/polardbxclusters
		v1.GET("/clusters", domain_pxc.List)
		v1.POST("/clusters", domain_pxc.Create)
		v1.POST("/clusters/:namespace/create", domain_pxc.CreateFromConfig)
		v1.GET("/clusters/:namespace/:name", domain_pxc.Get)
		v1.PATCH("/clusters/:namespace/:name/log-config/:nodeType", domain_pxc.UpdateLogConfig)
		v1.PATCH("/clusters/:namespace/:name/scale", domain_pxc.Scale)
		v1.PATCH("/clusters/:namespace/:name/upgrade", domain_pxc.Upgrade)
		v1.DELETE("/clusters/:namespace/:name", domain_pxc.Delete)
		v1.PUT("/clusters/:namespace/:name", domain_pxc.Update)
		v1.GET("/clusters/:namespace/:name/alerts-summary", domain_pxc.GetAlertsSummary)

		// Pods (保持原实现)
		v1.GET("/pods/:namespace/:name/exec", api_pod.ExecWS)
		v1.GET("/pods/:namespace/:name", api_pod.Get)
		v1.DELETE("/pods/:namespace/:name", api_pod.Delete)
		v1.GET("/clusters/:namespace/:name/pods", api_pod.ListForCluster)
		v1.GET("/pods", api_pod.List)

		// Backups under cluster
		v1.GET("/clusters/:namespace/:name/backups", domain_pxc.ListBackups)
		v1.POST("/clusters/:namespace/:name/backups", domain_pxc.CreateBackup)
		// Root-level backup ops（保留原行为）
		v1.POST("/backups/validate", domain_pxc.ValidateBackup)
		v1.GET("/backups/:namespace/:name/stream", domain_pxc.StreamBackupEvents)
		v1.GET("/backups/:namespace/:name/metrics", domain_pxc.GetBackupMetrics)
		v1.DELETE("/backups/:namespace/:name", domain_pxc.DeleteBackup)
		v1.POST("/backups/:namespace/:name/force-delete", domain_pxc.ForceDeleteBackup)
		v1.GET("/backups/overview", domain_pxc.GetBackupOverview)
		v1.GET("/backups/binlog/metrics", domain_pxc.GetBinlogMetrics)

		// Settings for dashboard thresholds
		v1.GET("/settings/backup-dashboard", api_settings.Get)
		v1.PUT("/settings/backup-dashboard", api_settings.Update)
		// Generic settings alias (for frontend compatibility)
		v1.GET("/settings", api_settings.Get)
		v1.PUT("/settings", api_settings.Update)

		// XStore Backup alias routes (frontend expects /xstore-backups)
		v1.GET("/xstore-backups", domain_xs.ListBackups)
		v1.POST("/xstore-backups", domain_xs.CreateBackup)
		v1.GET("/xstore-backups/:namespace/:name", domain_xs.GetBackup)
		v1.PUT("/xstore-backups/:namespace/:name", domain_xs.UpdateBackup)
		v1.DELETE("/xstore-backups/:namespace/:name", domain_xs.DeleteBackup)
		v1.POST("/xstore-backups/:namespace/:name/force-delete", domain_xs.ForceDeleteBackup)

		// XStore Follower alias routes (frontend expects /xstore-followers)
		v1.GET("/xstore-followers", domain_xs.ListFollowers)
		v1.POST("/xstore-followers", domain_xs.CreateFollower)
		v1.GET("/xstore-followers/:namespace/:name", domain_xs.GetFollower)
		v1.PUT("/xstore-followers/:namespace/:name", domain_xs.UpdateFollower)
		v1.DELETE("/xstore-followers/:namespace/:name", domain_xs.DeleteFollower)

		// System module（前端依赖：/api/v1/system/*）
		v1.GET("/system/context", api_system.ContextInfo)
		v1.GET("/system/namespaces", api_system.ListNamespaces)

		// Direct routes for frontend compatibility
		v1.GET("/namespaces", api_system.ListNamespaces)                              // Maps to /platform/system/namespaces
		v1.GET("/prometheus-rules", api_prometheusrule.List)                          // PrometheusRule resources
		v1.GET("/prometheus-rules/:namespace/:name/yaml", api_prometheusrule.GetYAML) // Get YAML
		v1.POST("/prometheus-rules/validate", api_prometheusrule.ValidateRule)        // Validate YAML

		// Pre-change safety checklist
		v1.GET("/clusters/:namespace/:name/prechange-check", domain_pxc.GetPrechangeChecklist)
		v1.POST("/clusters/:namespace/:name/precheck", domain_pxc.Precheck)
		v1.GET("/alerts", api_alerts.List)

		// Logs endpoint (pod logs)
		v1.GET("/logs/:namespace/:pod_name", api_pod.GetLogs)

		// Parameter routes → domain
		v1.GET("/parameters", domain_pxc.ListParameters)
		v1.POST("/parameters", domain_pxc.CreateParameter)
		v1.GET("/parameters/:name", domain_pxc.GetParameter)
		v1.PUT("/parameters/:name", domain_pxc.UpdateParameter)
		v1.DELETE("/parameters/:name", domain_pxc.DeleteParameter)

		// Monitor routes (保持原实现)
		v1.GET("/monitors", api_monitor.List)
		v1.POST("/monitors", api_monitor.Create)
		v1.GET("/monitors/:namespace/:name", api_monitor.Get)
		v1.PUT("/monitors/:namespace/:name", api_monitor.Update)
		v1.DELETE("/monitors/:namespace/:name", api_monitor.Delete)

		// BackupSchedule routes → domain
		v1.GET("/backup-schedules", domain_pxc.ListSchedules)
		v1.POST("/backup-schedules", domain_pxc.CreateSchedule)
		v1.GET("/backup-schedules/:namespace/:name", domain_pxc.GetSchedule)
		v1.PUT("/backup-schedules/:namespace/:name", domain_pxc.UpdateSchedule)
		v1.DELETE("/backup-schedules/:namespace/:name", domain_pxc.DeleteSchedule)

		// Diagnostics routes (保持原实现)
		v1.POST("/diagnostics/:namespace/:cluster/start", api_diagnostics.Start)
		v1.GET("/diagnostics/:namespace/:id/status", api_diagnostics.GetStatus)
		v1.GET("/diagnostics/reports", api_diagnostics.ListReports)
		v1.GET("/diagnostics/:namespace/:id/download", api_diagnostics.Download)

		// ParameterTemplate routes → domain
		v1.GET("/parameter-templates", domain_pxc.ListTemplates)
		v1.POST("/parameter-templates", domain_pxc.CreateTemplate)
		v1.GET("/parameter-templates/:namespace/:name", domain_pxc.GetTemplate)
		v1.PUT("/parameter-templates/:namespace/:name", domain_pxc.UpdateTemplate)
		v1.DELETE("/parameter-templates/:namespace/:name", domain_pxc.DeleteTemplate)

		// ClusterKnobs routes → clusterknobs handlers (add direct aliases for frontend)
		v1.GET("/cluster-knobs", api_clusterknobs.GetList)
		v1.POST("/cluster-knobs", api_clusterknobs.Create)
		v1.GET("/cluster-knobs/:namespace/:name", api_clusterknobs.Get)
		v1.PUT("/cluster-knobs/:namespace/:name", api_clusterknobs.Update)
		v1.DELETE("/cluster-knobs/:namespace/:name", api_clusterknobs.Delete)

		// SystemTask routes → domain/systemtasks
		v1.GET("/system-tasks", domain_st.List)
		v1.POST("/system-tasks", domain_st.Create)
		v1.GET("/system-tasks/:namespace/:name", domain_st.Get)
		v1.PUT("/system-tasks/:namespace/:name", domain_st.Update)
		v1.DELETE("/system-tasks/:namespace/:name", domain_st.Delete)

		// LogCollector routes（保持原实现）
		v1.GET("/log-collectors", api_logcollector.List)
		v1.POST("/log-collectors", api_logcollector.Create)
		v1.GET("/log-collectors/:namespace/:name", api_logcollector.Get)
		v1.PUT("/log-collectors/:namespace/:name", api_logcollector.Update)
		v1.DELETE("/log-collectors/:namespace/:name", api_logcollector.Delete)
		v1.GET("/log-collectors/:namespace/pipeline", api_logcollector.GetLogstashPipeline)
		v1.PUT("/log-collectors/:namespace/pipeline", api_logcollector.UpdateLogstashPipeline)
		v1.GET("/log-collectors/:namespace/elastic-certs", api_logcollector.GetElasticsearchCert)
		v1.PUT("/log-collectors/:namespace/elastic-certs", api_logcollector.UpdateElasticsearchCert)
		v1.GET("/log-collectors/:namespace/:name/status", api_logcollector.GetLogCollectorStatus)
		v1.GET("/log-collectors/:namespace/logstash/logs", api_logcollector.StreamLogstashLogs)
		v1.POST("/log-collectors/:namespace/test", api_logcollector.TestLogCollector)

		// Log Service / Strategy / Logs（保持原实现）
		v1.GET("/log-service/status", api_logservice.Status)
		v1.GET("/log-strategies", api_logstrategy.List)
		v1.POST("/log-strategies", api_logstrategy.Create)
		v1.POST("/log-strategies/precheck", api_logstrategy.Precheck)
		v1.GET("/log-strategies/apply-records", api_logstrategy.ListApplyRecords) // New endpoint
		v1.GET("/log-strategies/:name", api_logstrategy.Get)
		v1.PUT("/log-strategies/:name", api_logstrategy.Update)
		v1.DELETE("/log-strategies/:name", api_logstrategy.Delete)
		v1.POST("/log-strategies/:name/apply", api_logstrategy.Apply)
		v1.POST("/log-strategies/test-connection", api_logstrategy.TestConnection)
		// Logs Bootstrap (安装向导后端)
		v1.POST("/logs/bootstrap", api_logs.Bootstrap)
		v1.GET("/logs/bootstrap/status", api_logs.BootstrapStatus)
		v1.GET("/logs/bootstrap/logs", api_logs.BootstrapLogs)
		v1.POST("/logs/query", api_logs.Query)
		v1.GET("/logs/presets", api_logs.Presets)
		v1.GET("/logs/presets/:pattern", api_logs.PresetByPattern)

		// Monitoring / Grafana（保持原实现）
		v1.POST("/monitoring/bootstrap", api_monitoring.Bootstrap)
		v1.GET("/monitoring/status", api_monitoring.Status)
		v1.GET("/monitoring/preflight", api_monitoring.Preflight)
		v1.DELETE("/monitoring/uninstall", api_monitoring.Uninstall)
		v1.GET("/monitoring/grafana/config", api_grafana.GetConfig)
		v1.PUT("/monitoring/grafana/config", api_grafana.PutConfig)
		v1.POST("/monitoring/grafana/dashboards/sync", api_grafana.SyncDashboards)
		v1.GET("/monitoring/grafana/dashboards", api_grafana.ListDashboards)
		v1.GET("/monitoring/grafana/dashboards/:name/versions", api_grafana.ListDashboardVersions)
		v1.POST("/monitoring/grafana/dashboards/:name/rollback", api_grafana.RollbackDashboard)

		// BackupBinlog routes（改由 domain handlers 接管，路径保持不变）
		v1.GET("/backup-binlogs", domain_pxc.ListBackupBinlogs)
		v1.POST("/backup-binlogs", domain_pxc.CreateBackupBinlog)
		v1.GET("/backup-binlogs/:namespace/:name", domain_pxc.GetBackupBinlog)
		v1.PUT("/backup-binlogs/:namespace/:name", domain_pxc.UpdateBackupBinlog)
		v1.DELETE("/backup-binlogs/:namespace/:name", domain_pxc.DeleteBackupBinlog)

		// HPFS sinks
		v1.GET("/hpfs/sinks", domain_pxc.ListHpfsSinks)
		v1.POST("/hpfs/sinks/validate", domain_pxc.ValidateHpfsSink)

		// Backup advice
		v1.GET("/clusters/:namespace/:name/backup-advice", domain_pxc.GetBackupAdvice)

		// Restore API routes（保持原实现路径）
		v1.POST("/clusters/:namespace/:name/restore", api_restore.RestoreCluster)
		v1.POST("/clusters/:namespace/:name/pitr", api_restore.InitiatePITR)
		v1.GET("/clusters/:namespace/:name/restore-status", api_restore.GetRestoreStatus)
		v1.GET("/restore-jobs", api_restore.ListJobs)
		v1.GET("/restore-jobs/:namespace/:name", api_restore.GetJob)
		v1.DELETE("/restore-jobs/:namespace/:name", api_restore.CancelJob)
	}

	// Register CRD-aligned alias routes (no behavior change)
	api_router.RegisterCRDAliasRoutes(v1)
	// Register domain entrance aliases (safe, non-conflicting)
	api_router.RegisterDomainRoutes(v1)

	// Grouped route logging for discoverability
	api_router.LogGroupedRoutes(r)

	for _, route := range r.Routes() {
		log.Printf("Registered route: %s %s", route.Method, route.Path)
	}

	if err := r.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
