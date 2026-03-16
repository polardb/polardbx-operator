package router

import (
	domain_restore "polardbx-dashboard-backend/pkg/api/domain/platform/restore/handler"

	"github.com/gin-gonic/gin"
)

// RegisterRestoreRoutes registers restore-related routes
func RegisterRestoreRoutes(v1 *gin.RouterGroup) {
	reg := NewRouteRegistry()
	RegisterRestoreRoutesRegistry(reg)
	reg.Apply(v1)
}

func RegisterRestoreRoutesRegistry(reg *RouteRegistry) {
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "Restore and PITR routes",
		Routes: []Route{
			// Cluster restore operations
			{Method: "POST", Path: "/clusters/:namespace/:name/restore", Handler: domain_restore.RestoreCluster},
			{Method: "POST", Path: "/clusters/:namespace/:name/pitr", Handler: domain_restore.InitiatePITR},
			{Method: "GET", Path: "/clusters/:namespace/:name/restore-status", Handler: domain_restore.GetRestoreStatus},

			// Restore jobs
			{Method: "GET", Path: "/restore-jobs", Handler: domain_restore.ListJobs},
			{Method: "GET", Path: "/restore-jobs/:namespace/:name", Handler: domain_restore.GetJob},
			{Method: "DELETE", Path: "/restore-jobs/:namespace/:name", Handler: domain_restore.CancelJob},
		},
	})
}
