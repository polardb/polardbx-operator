package router

import (
	domain_logcollector "polardbx-dashboard-backend/pkg/api/domain/platform/logcollector/handler"
	domain_logs "polardbx-dashboard-backend/pkg/api/domain/platform/logs/handler"
	domain_logservice "polardbx-dashboard-backend/pkg/api/domain/platform/logservice/handler"
	domain_logstrategy "polardbx-dashboard-backend/pkg/api/domain/platform/logstrategy/handler"

	"github.com/gin-gonic/gin"
)

// RegisterLogsRoutes registers log-related routes
func RegisterLogsRoutes(v1 *gin.RouterGroup) {
	reg := NewRouteRegistry()
	RegisterLogsRoutesRegistry(reg)
	reg.Apply(v1)
}

func RegisterLogsRoutesRegistry(reg *RouteRegistry) {
	reg.RegisterGroup(RouteGroup{
		Prefix:      "",
		Description: "Log collector, log service, and log strategy routes",
		Routes: []Route{
			// Log Collectors
			{Method: "GET", Path: "/log-collectors", Handler: domain_logcollector.List},
			{Method: "POST", Path: "/log-collectors", Handler: domain_logcollector.Create},
			{Method: "GET", Path: "/log-collectors/:namespace/:name", Handler: domain_logcollector.Get},
			{Method: "PUT", Path: "/log-collectors/:namespace/:name", Handler: domain_logcollector.Update},
			{Method: "DELETE", Path: "/log-collectors/:namespace/:name", Handler: domain_logcollector.Delete},
			{Method: "GET", Path: "/log-collectors/:namespace/pipeline", Handler: domain_logcollector.GetLogstashPipeline},
			{Method: "PUT", Path: "/log-collectors/:namespace/pipeline", Handler: domain_logcollector.UpdateLogstashPipeline},
			{Method: "GET", Path: "/log-collectors/:namespace/elastic-certs", Handler: domain_logcollector.GetElasticsearchCert},
			{Method: "PUT", Path: "/log-collectors/:namespace/elastic-certs", Handler: domain_logcollector.UpdateElasticsearchCert},
			{Method: "GET", Path: "/log-collectors/:namespace/:name/status", Handler: domain_logcollector.GetLogCollectorStatus},
			{Method: "GET", Path: "/log-collectors/:namespace/logstash/logs", Handler: domain_logcollector.StreamLogstashLogs},
			{Method: "POST", Path: "/log-collectors/:namespace/test", Handler: domain_logcollector.TestLogCollector},

			// Log Service
			{Method: "GET", Path: "/log-service/status", Handler: domain_logservice.Status},

			// Log Strategies
			{Method: "GET", Path: "/log-strategies", Handler: domain_logstrategy.List},
			{Method: "POST", Path: "/log-strategies", Handler: domain_logstrategy.Create},
			{Method: "POST", Path: "/log-strategies/precheck", Handler: domain_logstrategy.Precheck},
			{Method: "GET", Path: "/log-strategies/apply-records", Handler: domain_logstrategy.ListApplyRecords},
			{Method: "GET", Path: "/log-strategies/:name", Handler: domain_logstrategy.Get},
			{Method: "PUT", Path: "/log-strategies/:name", Handler: domain_logstrategy.Update},
			{Method: "DELETE", Path: "/log-strategies/:name", Handler: domain_logstrategy.Delete},
			{Method: "POST", Path: "/log-strategies/:name/apply", Handler: domain_logstrategy.Apply},
			{Method: "POST", Path: "/log-strategies/test-connection", Handler: domain_logstrategy.TestConnection},

			// Logs Bootstrap (installation wizard)
			{Method: "POST", Path: "/logs/bootstrap", Handler: domain_logs.Bootstrap},
			{Method: "GET", Path: "/logs/bootstrap/status", Handler: domain_logs.BootstrapStatus},
			{Method: "GET", Path: "/logs/bootstrap/logs", Handler: domain_logs.BootstrapLogs},
			{Method: "POST", Path: "/logs/query", Handler: domain_logs.Query},
			{Method: "GET", Path: "/logs/presets", Handler: domain_logs.Presets},
			{Method: "GET", Path: "/logs/presets/:pattern", Handler: domain_logs.PresetByPattern},
		},
	})
}
