package router

import (
	domain_monitoring "polardbx-dashboard-backend/pkg/api/domain/monitoring"
	domain_grafana "polardbx-dashboard-backend/pkg/api/domain/platform/grafana/handler"
	domain_prometheusrule "polardbx-dashboard-backend/pkg/api/domain/platform/prometheusrule/handler"

	"github.com/gin-gonic/gin"
)

// RegisterMonitoringRoutes registers monitoring-related routes
func RegisterMonitoringRoutes(v1 *gin.RouterGroup) {
	reg := NewRouteRegistry()
	RegisterMonitoringRoutesRegistry(reg)
	reg.Apply(v1)
}

func RegisterMonitoringRoutesRegistry(reg *RouteRegistry) {
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "Monitoring and Grafana routes",
		Routes: []Route{
			// Monitor CRD routes
			{Method: "GET", Path: "/monitors", Handler: domain_monitoring.ListMonitors},
			{Method: "POST", Path: "/monitors", Handler: domain_monitoring.CreateMonitor},
			{Method: "GET", Path: "/monitors/:namespace/:name", Handler: domain_monitoring.GetMonitor},
			{Method: "PUT", Path: "/monitors/:namespace/:name", Handler: domain_monitoring.UpdateMonitor},
			{Method: "DELETE", Path: "/monitors/:namespace/:name", Handler: domain_monitoring.DeleteMonitor},

			// Monitoring workflow endpoints
			{Method: "POST", Path: "/monitoring/bootstrap", Handler: domain_monitoring.Bootstrap},
			{Method: "GET", Path: "/monitoring/bootstrap/status", Handler: domain_monitoring.BootstrapStatus},
			{Method: "GET", Path: "/monitoring/bootstrap/logs", Handler: domain_monitoring.GetBootstrapLogs},
			{Method: "GET", Path: "/monitoring/status", Handler: domain_monitoring.Status},
			{Method: "GET", Path: "/monitoring/preflight", Handler: domain_monitoring.DetectEnvironment},
			{Method: "DELETE", Path: "/monitoring/uninstall", Handler: domain_monitoring.Uninstall},

			// Monitoring v2 workflow endpoints
			{Method: "GET", Path: "/monitoring/detect", Handler: domain_monitoring.DetectEnvironment},
			{Method: "POST", Path: "/monitoring/plan", Handler: domain_monitoring.CreatePlan},
			{Method: "POST", Path: "/monitoring/install", Handler: domain_monitoring.StartInstallation},
			{Method: "GET", Path: "/monitoring/install/:sessionId/status", Handler: domain_monitoring.GetInstallStatus},
			{Method: "GET", Path: "/monitoring/install/:sessionId/logs", Handler: domain_monitoring.GetBootstrapLogs},
			{Method: "POST", Path: "/monitoring/install/:sessionId/retry", Handler: domain_monitoring.TriggerRetry},
			{Method: "POST", Path: "/monitoring/diagnose", Handler: domain_monitoring.DiagnoseFailure},
			{Method: "POST", Path: "/monitoring/auto-fix", Handler: domain_monitoring.ApplyAutoFix},

			// Grafana routes
			{Method: "GET", Path: "/monitoring/grafana/config", Handler: domain_grafana.GetConfig},
			{Method: "PUT", Path: "/monitoring/grafana/config", Handler: domain_grafana.PutConfig},
			{Method: "POST", Path: "/monitoring/grafana/dashboards/sync", Handler: domain_grafana.SyncDashboards},
			{Method: "GET", Path: "/monitoring/grafana/dashboards", Handler: domain_grafana.ListDashboards},
			{Method: "GET", Path: "/monitoring/grafana/dashboards/:name/versions", Handler: domain_grafana.ListDashboardVersions},
			{Method: "POST", Path: "/monitoring/grafana/dashboards/:name/rollback", Handler: domain_grafana.RollbackDashboard},
			{Method: "GET", Path: "/monitoring/grafana/templates", Handler: domain_grafana.ListTemplates},
			{Method: "GET", Path: "/monitoring/grafana/templates/:name", Handler: domain_grafana.GetTemplate},

			// Prometheus Rules
			{Method: "GET", Path: "/prometheus-rules", Handler: domain_prometheusrule.List},
			{Method: "GET", Path: "/prometheus-rules/:namespace/:name/yaml", Handler: domain_prometheusrule.GetYAML},
			{Method: "POST", Path: "/prometheus-rules/validate", Handler: domain_prometheusrule.ValidateRule},
			{Method: "GET", Path: "/prometheus-rules/templates", Handler: domain_prometheusrule.ListTemplates},
			{Method: "GET", Path: "/prometheus-rules/templates/:name", Handler: domain_prometheusrule.GetTemplate},
			{Method: "POST", Path: "/prometheus-rules/apply", Handler: domain_prometheusrule.ApplyTemplate},
		},
	})
}
