package router

import (
	domain_system "polardbx-dashboard-backend/pkg/api/domain/platform/system/handler"
	domain_st "polardbx-dashboard-backend/pkg/api/domain/systemtasks"

	"github.com/gin-gonic/gin"
)

// RegisterSystemRoutes registers system-related routes
func RegisterSystemRoutes(v1 *gin.RouterGroup) {
	reg := NewRouteRegistry()
	RegisterSystemRoutesRegistry(reg)
	reg.Apply(v1)
}

func RegisterSystemRoutesRegistry(reg *RouteRegistry) {
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "System and system task routes",
		Routes: []Route{
			// System context
			{Method: "GET", Path: "/system/context", Handler: domain_system.ContextInfo},
			{Method: "GET", Path: "/system/namespaces", Handler: domain_system.ListNamespaces},

			// Direct namespace route for frontend compatibility
			{Method: "GET", Path: "/namespaces", Handler: domain_system.ListNamespaces},

			// System Tasks
			{Method: "GET", Path: "/system-tasks", Handler: domain_st.List},
			{Method: "POST", Path: "/system-tasks", Handler: domain_st.Create},
			{Method: "GET", Path: "/system-tasks/:namespace/:name", Handler: domain_st.Get},
			{Method: "PUT", Path: "/system-tasks/:namespace/:name", Handler: domain_st.Update},
			{Method: "DELETE", Path: "/system-tasks/:namespace/:name", Handler: domain_st.Delete},
		},
	})
}
