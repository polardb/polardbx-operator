package router

import (
	domain_alerts "polardbx-dashboard-backend/pkg/api/domain/platform/alerts/handler"

	"github.com/gin-gonic/gin"
)

// RegisterAlertsRoutes registers alert-related routes used by the dashboard UI.
func RegisterAlertsRoutes(v1 *gin.RouterGroup) {
	reg := NewRouteRegistry()
	RegisterAlertsRoutesRegistry(reg)
	reg.Apply(v1)
}

func RegisterAlertsRoutesRegistry(reg *RouteRegistry) {
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "Alertmanager config, routes, silences and receiver wizard routes",
		Routes: []Route{
			// NOTE: GET /alerts is already registered in cluster routes; do not duplicate.

			// Profiles
			{Method: "GET", Path: "/alerts/profiles", Handler: domain_alerts.ListProfiles},
			{Method: "POST", Path: "/alerts/profiles", Handler: domain_alerts.CreateProfile},
			{Method: "GET", Path: "/alerts/profiles/:name", Handler: domain_alerts.GetProfile},
			{Method: "PUT", Path: "/alerts/profiles/:name", Handler: domain_alerts.UpdateProfile},
			{Method: "DELETE", Path: "/alerts/profiles/:name", Handler: domain_alerts.DeleteProfile},
			{Method: "POST", Path: "/alerts/profiles/dry-run", Handler: domain_alerts.DryRunProfile},

			// Routes (and default Alertmanager URL storage)
			{Method: "GET", Path: "/alerts/routes", Handler: domain_alerts.GetRoutes},
			{Method: "PUT", Path: "/alerts/routes", Handler: domain_alerts.PutRoutes},

			// Silences (alertmanager query param optional; backend falls back to stored URL)
			{Method: "GET", Path: "/alerts/silences", Handler: domain_alerts.ListSilences},
			{Method: "POST", Path: "/alerts/silences", Handler: domain_alerts.CreateSilence},
			{Method: "DELETE", Path: "/alerts/silences/:id", Handler: domain_alerts.DeleteSilence},

			// Test alert (alertmanager query param optional; backend falls back to stored URL)
			{Method: "POST", Path: "/alerts/test", Handler: domain_alerts.TestAlert},

			// Receiver wizard (AlertmanagerConfig-based)
			{Method: "POST", Path: "/alerts/receivers/apply", Handler: domain_alerts.ApplyReceiverConfig},
		},
	})
}
