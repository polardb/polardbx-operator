package platform

import (
	"github.com/gin-gonic/gin"
)

// RegisterRoutes adds /api/v1/platform/* aliases for cross-cutting platform endpoints.
func RegisterRoutes(v1 *gin.RouterGroup) {
	p := v1.Group("/platform")
	// System info
	p.GET("/system/context", SystemContext)
	p.GET("/system/namespaces", ListNamespaces)
	// Monitoring
	p.POST("/monitoring/bootstrap", MonitoringBootstrap)
	p.GET("/monitoring/bootstrap/status", MonitoringBootstrapStatus)
	p.GET("/monitoring/bootstrap/logs", MonitoringBootstrapLogs)
	p.GET("/monitoring/status", MonitoringStatus)
	p.GET("/monitoring/preflight", MonitoringPreflight)
	p.DELETE("/monitoring/uninstall", MonitoringUninstall)
	// Grafana
	p.GET("/grafana/config", GrafanaGetConfig)
	p.PUT("/grafana/config", GrafanaPutConfig)
	p.POST("/grafana/dashboards/sync", GrafanaSyncDashboards)
	p.GET("/grafana/dashboards", GrafanaListDashboards)
	p.GET("/grafana/dashboards/:name/versions", GrafanaListVersions)
	p.POST("/grafana/dashboards/:name/rollback", GrafanaRollback)
	// Logs
	p.POST("/logs/query", LogsQuery)
	p.GET("/logs/presets", LogsPresets)
	p.GET("/logs/presets/:pattern", LogsPresetByPattern)
	// Log collection bootstrap
	p.POST("/logs/bootstrap", LogsBootstrap)
	p.GET("/logs/bootstrap/status", LogsBootstrapStatus)
	p.GET("/logs/bootstrap/logs", LogsBootstrapLogs)
	// Log service / strategy
	p.GET("/log-service/status", LogServiceStatus)
	p.GET("/log-strategies", LogStrategyList)
	p.POST("/log-strategies", LogStrategyCreate)
	p.POST("/log-strategies/precheck", LogStrategyPrecheck)
	p.GET("/log-strategies/:name", LogStrategyGet)
	p.PUT("/log-strategies/:name", LogStrategyUpdate)
	p.DELETE("/log-strategies/:name", LogStrategyDelete)
	p.POST("/log-strategies/:name/apply", LogStrategyApply)
	// Pods
	p.GET("/pods", PodList)
	p.GET("/pods/:namespace/:name", PodGet)
	p.GET("/pods/:namespace/:name/exec", PodExecWS)
	p.DELETE("/pods/:namespace/:name", PodDelete)
	p.GET("/logs/:namespace/:pod_name", PodGetLogs)
	// Alerts
	p.GET("/alerts", AlertsList)
	p.GET("/alerts/profiles", AlertsListProfiles)
	p.POST("/alerts/profiles", AlertsCreateProfile)
	p.GET("/alerts/profiles/:name", AlertsGetProfile)
	p.PUT("/alerts/profiles/:name", AlertsUpdateProfile)
	p.DELETE("/alerts/profiles/:name", AlertsDeleteProfile)
	p.POST("/alerts/profiles/dry-run", AlertsDryRunProfile)
	p.GET("/alerts/routes", AlertsGetRoutes)
	p.PUT("/alerts/routes", AlertsPutRoutes)
	p.GET("/alerts/silences", AlertsListSilences)
	p.POST("/alerts/silences", AlertsCreateSilence)
	p.DELETE("/alerts/silences/:id", AlertsDeleteSilence)
	p.POST("/alerts/test", AlertsTest)
	// Settings
	p.GET("/settings/backup-dashboard", GetBackupDashboard)
	p.PUT("/settings/backup-dashboard", PutBackupDashboard)
}
