package platform

import (
	domain_monitoring "polardbx-dashboard-backend/pkg/api/domain/monitoring"
	domain_alerts "polardbx-dashboard-backend/pkg/api/domain/platform/alerts/handler"
	domain_grafana "polardbx-dashboard-backend/pkg/api/domain/platform/grafana/handler"
	domain_logs "polardbx-dashboard-backend/pkg/api/domain/platform/logs/handler"
	domain_logservice "polardbx-dashboard-backend/pkg/api/domain/platform/logservice/handler"
	domain_logstrategy "polardbx-dashboard-backend/pkg/api/domain/platform/logstrategy/handler"
	domain_pod "polardbx-dashboard-backend/pkg/api/domain/platform/pod/handler"
	domain_prometheusrule "polardbx-dashboard-backend/pkg/api/domain/platform/prometheusrule/handler"
	domain_settings "polardbx-dashboard-backend/pkg/api/domain/platform/settings/handler"
	domain_system "polardbx-dashboard-backend/pkg/api/domain/platform/system/handler"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes adds /api/v1/platform/* aliases for cross-cutting platform endpoints.
func RegisterRoutes(v1 *gin.RouterGroup) {
	p := v1.Group("/platform")
	// System info
	p.GET("/system/context", domain_system.ContextInfo)
	p.GET("/system/namespaces", domain_system.ListNamespaces)
	p.GET("/system/storage-classes", domain_system.ListStorageClasses)
	p.GET("/system/polardbx-versions", domain_system.ListPolarDBXVersions)
	// Monitoring
	p.POST("/monitoring/bootstrap", domain_monitoring.Bootstrap)
	p.GET("/monitoring/bootstrap/status", domain_monitoring.BootstrapStatus)
	p.GET("/monitoring/bootstrap/logs", domain_monitoring.GetBootstrapLogs)
	p.GET("/monitoring/status", domain_monitoring.Status)
	p.GET("/monitoring/preflight", domain_monitoring.DetectEnvironment)
	p.DELETE("/monitoring/uninstall", domain_monitoring.Uninstall)
	// Grafana
	p.GET("/grafana/config", domain_grafana.GetConfig)
	p.PUT("/grafana/config", domain_grafana.PutConfig)
	p.POST("/grafana/dashboards/sync", domain_grafana.SyncDashboards)
	p.GET("/grafana/dashboards", domain_grafana.ListDashboards)
	p.GET("/grafana/dashboards/:name/versions", domain_grafana.ListDashboardVersions)
	p.POST("/grafana/dashboards/:name/rollback", domain_grafana.RollbackDashboard)
	p.GET("/grafana/templates", domain_grafana.ListTemplates)
	p.GET("/grafana/templates/:name", domain_grafana.GetTemplate)
	// PrometheusRule templates
	p.GET("/prometheus-rules/templates", domain_prometheusrule.ListTemplates)
	p.GET("/prometheus-rules/templates/:name", domain_prometheusrule.GetTemplate)
	p.POST("/prometheus-rules/templates/apply", domain_prometheusrule.ApplyTemplate)
	// Logs
	p.POST("/logs/query", domain_logs.Query)
	p.GET("/logs/presets", domain_logs.Presets)
	p.GET("/logs/presets/:pattern", domain_logs.PresetByPattern)
	// Log collection bootstrap
	p.POST("/logs/bootstrap", domain_logs.Bootstrap)
	p.GET("/logs/bootstrap/status", domain_logs.BootstrapStatus)
	p.GET("/logs/bootstrap/logs", domain_logs.BootstrapLogs)
	// Log service / strategy
	p.GET("/log-service/status", domain_logservice.Status)
	p.GET("/log-strategies", domain_logstrategy.List)
	p.POST("/log-strategies", domain_logstrategy.Create)
	p.POST("/log-strategies/precheck", domain_logstrategy.Precheck)
	p.GET("/log-strategies/:name", domain_logstrategy.Get)
	p.PUT("/log-strategies/:name", domain_logstrategy.Update)
	p.DELETE("/log-strategies/:name", domain_logstrategy.Delete)
	p.POST("/log-strategies/:name/apply", domain_logstrategy.Apply)
	// Pods
	p.GET("/pods", domain_pod.List)
	p.GET("/pods/:namespace/:name", domain_pod.Get)
	p.GET("/pods/:namespace/:name/exec", domain_pod.ExecWS)
	p.DELETE("/pods/:namespace/:name", domain_pod.Delete)
	p.GET("/logs/:namespace/:pod_name", domain_pod.GetLogs)
	// Alerts
	p.GET("/alerts", domain_alerts.List)
	p.GET("/alerts/profiles", domain_alerts.ListProfiles)
	p.POST("/alerts/profiles", domain_alerts.CreateProfile)
	p.GET("/alerts/profiles/:name", domain_alerts.GetProfile)
	p.PUT("/alerts/profiles/:name", domain_alerts.UpdateProfile)
	p.DELETE("/alerts/profiles/:name", domain_alerts.DeleteProfile)
	p.POST("/alerts/profiles/dry-run", domain_alerts.DryRunProfile)
	p.GET("/alerts/routes", domain_alerts.GetRoutes)
	p.PUT("/alerts/routes", domain_alerts.PutRoutes)
	p.GET("/alerts/silences", domain_alerts.ListSilences)
	p.POST("/alerts/silences", domain_alerts.CreateSilence)
	p.DELETE("/alerts/silences/:id", domain_alerts.DeleteSilence)
	p.POST("/alerts/test", domain_alerts.TestAlert)
	// Settings
	p.GET("/settings/backup-dashboard", domain_settings.Get)
	p.PUT("/settings/backup-dashboard", domain_settings.Update)
}
