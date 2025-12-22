package router

import (
	domain_alerts "polardbx-dashboard-backend/pkg/api/domain/platform/alerts/handler"
	domain_clusterknobs "polardbx-dashboard-backend/pkg/api/domain/platform/clusterknobs/handler"
	domain_pod "polardbx-dashboard-backend/pkg/api/domain/platform/pod/handler"
	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"

	"github.com/gin-gonic/gin"
)

// RegisterClusterRoutes registers cluster-related routes
func RegisterClusterRoutes(v1 *gin.RouterGroup) {
	reg := NewRouteRegistry()
	RegisterClusterRoutesRegistry(reg)
	reg.Apply(v1)
}

func RegisterClusterRoutesRegistry(reg *RouteRegistry) {
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "Cluster, pod, alerts, and parameter routes",
		Routes: []Route{
			// Cluster CRUD
			{Method: "GET", Path: "/clusters", Handler: domain_pxc.List},
			{Method: "POST", Path: "/clusters", Handler: domain_pxc.Create},
			{Method: "POST", Path: "/clusters/:namespace/create", Handler: domain_pxc.CreateFromConfig},
			{Method: "GET", Path: "/clusters/:namespace/:name", Handler: domain_pxc.Get},
			{Method: "PUT", Path: "/clusters/:namespace/:name", Handler: domain_pxc.Update},
			{Method: "DELETE", Path: "/clusters/:namespace/:name", Handler: domain_pxc.Delete},

			// Cluster operations
			{Method: "PATCH", Path: "/clusters/:namespace/:name/log-config/:nodeType", Handler: domain_pxc.UpdateLogConfig},
			{Method: "PATCH", Path: "/clusters/:namespace/:name/scale", Handler: domain_pxc.Scale},
			{Method: "PATCH", Path: "/clusters/:namespace/:name/upgrade", Handler: domain_pxc.Upgrade},
			{Method: "GET", Path: "/clusters/:namespace/:name/upgrade-plan", Handler: domain_pxc.GetUpgradePlan},
			{Method: "GET", Path: "/clusters/:namespace/:name/alerts-summary", Handler: domain_pxc.GetAlertsSummary},
			{Method: "GET", Path: "/clusters/:namespace/:name/resource-usage", Handler: domain_pxc.GetResourceUsage},
			{Method: "GET", Path: "/clusters/:namespace/:name/prechange-check", Handler: domain_pxc.GetPrechangeChecklist},
			{Method: "POST", Path: "/clusters/:namespace/:name/precheck", Handler: domain_pxc.Precheck},

			// Pods
			{Method: "GET", Path: "/pods", Handler: domain_pod.List},
			{Method: "GET", Path: "/pods/:namespace/:name", Handler: domain_pod.Get},
			{Method: "DELETE", Path: "/pods/:namespace/:name", Handler: domain_pod.Delete},
			{Method: "GET", Path: "/pods/:namespace/:name/exec", Handler: domain_pod.ExecWS},
			{Method: "GET", Path: "/clusters/:namespace/:name/pods", Handler: domain_pod.ListForCluster},
			{Method: "GET", Path: "/logs/:namespace/:pod_name", Handler: domain_pod.GetLogs},

			// Alerts
			{Method: "GET", Path: "/alerts", Handler: domain_alerts.List},
		},
	})

	// Parameters
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "Parameter CRUD routes",
		Routes: []Route{
			{Method: "GET", Path: "/parameters", Handler: domain_pxc.ListParameters},
			{Method: "POST", Path: "/parameters", Handler: domain_pxc.CreateParameter},
			{Method: "GET", Path: "/parameters/:name", Handler: domain_pxc.GetParameter},
			{Method: "PUT", Path: "/parameters/:name", Handler: domain_pxc.UpdateParameter},
			{Method: "DELETE", Path: "/parameters/:name", Handler: domain_pxc.DeleteParameter},
		},
	})

	// Parameter Templates
	baseTpl := "/parameter-templates"
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "Parameter template CRUD routes",
		Routes: []Route{
			{Method: "GET", Path: baseTpl, Handler: domain_pxc.ListTemplates},
			{Method: "POST", Path: baseTpl, Handler: domain_pxc.CreateTemplate},
			{Method: "GET", Path: baseTpl + "/:namespace/:name", Handler: domain_pxc.GetTemplate},
			{Method: "PUT", Path: baseTpl + "/:namespace/:name", Handler: domain_pxc.UpdateTemplate},
			{Method: "DELETE", Path: baseTpl + "/:namespace/:name", Handler: domain_pxc.DeleteTemplate},
		},
	})

	// Cluster Knobs
	baseKnobs := "/cluster-knobs"
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "Cluster knobs CRUD routes",
		Routes: []Route{
			{Method: "GET", Path: baseKnobs, Handler: domain_clusterknobs.GetList},
			{Method: "POST", Path: baseKnobs, Handler: domain_clusterknobs.Create},
			{Method: "GET", Path: baseKnobs + "/:namespace/:name", Handler: domain_clusterknobs.Get},
			{Method: "PUT", Path: baseKnobs + "/:namespace/:name", Handler: domain_clusterknobs.Update},
			{Method: "DELETE", Path: baseKnobs + "/:namespace/:name", Handler: domain_clusterknobs.Delete},
		},
	})
}
