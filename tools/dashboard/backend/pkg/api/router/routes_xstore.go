package router

import (
	domain_xs "polardbx-dashboard-backend/pkg/api/domain/xstores"

	"github.com/gin-gonic/gin"
)

// RegisterXStoreRoutes registers XStore-related routes
func RegisterXStoreRoutes(v1 *gin.RouterGroup) {
	reg := NewRouteRegistry()
	RegisterXStoreRoutesRegistry(reg)
	reg.Apply(v1)
}

func RegisterXStoreRoutesRegistry(reg *RouteRegistry) {
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "XStore backup and follower routes",
		Routes: []Route{
			// XStore Backups
			{Method: "GET", Path: "/xstore-backups", Handler: domain_xs.ListBackups},
			{Method: "POST", Path: "/xstore-backups", Handler: domain_xs.CreateBackup},
			{Method: "GET", Path: "/xstore-backups/:namespace/:name", Handler: domain_xs.GetBackup},
			{Method: "PUT", Path: "/xstore-backups/:namespace/:name", Handler: domain_xs.UpdateBackup},
			{Method: "DELETE", Path: "/xstore-backups/:namespace/:name", Handler: domain_xs.DeleteBackup},
			{Method: "POST", Path: "/xstore-backups/:namespace/:name/force-delete", Handler: domain_xs.ForceDeleteBackup},

			// XStore Followers
			{Method: "GET", Path: "/xstore-followers", Handler: domain_xs.ListFollowers},
			{Method: "POST", Path: "/xstore-followers", Handler: domain_xs.CreateFollower},
			{Method: "GET", Path: "/xstore-followers/:namespace/:name", Handler: domain_xs.GetFollower},
			{Method: "PUT", Path: "/xstore-followers/:namespace/:name", Handler: domain_xs.UpdateFollower},
			{Method: "DELETE", Path: "/xstore-followers/:namespace/:name", Handler: domain_xs.DeleteFollower},
		},
	})
}
