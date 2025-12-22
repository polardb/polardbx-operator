package router

import (
	domain_diagnostics "polardbx-dashboard-backend/pkg/api/domain/platform/diagnostics/handler"

	"github.com/gin-gonic/gin"
)

// RegisterDiagnosticsRoutes registers diagnostics-related routes
func RegisterDiagnosticsRoutes(v1 *gin.RouterGroup) {
	reg := NewRouteRegistry()
	RegisterDiagnosticsRoutesRegistry(reg)
	reg.Apply(v1)
}

func RegisterDiagnosticsRoutesRegistry(reg *RouteRegistry) {
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "Diagnostics routes",
		Routes: []Route{
			{Method: "POST", Path: "/diagnostics/:namespace/:cluster/start", Handler: domain_diagnostics.Start},
			{Method: "GET", Path: "/diagnostics/:namespace/:id/status", Handler: domain_diagnostics.GetStatus},
			{Method: "GET", Path: "/diagnostics/reports", Handler: domain_diagnostics.ListReports},
			{Method: "GET", Path: "/diagnostics/:namespace/:id/download", Handler: domain_diagnostics.Download},
			{Method: "GET", Path: "/diagnostics/:namespace/:id/file", Handler: domain_diagnostics.GetFile},
			{Method: "DELETE", Path: "/diagnostics/:namespace/:id", Handler: domain_diagnostics.DeleteJob},
		},
	})
}
