package router

import (
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

// routeKey builds a unique key for method+path for easy assertions.
func routeKey(method, path string) string {
	return method + " " + path
}

func collectRoutes(engine *gin.Engine) map[string]struct{} {
	out := make(map[string]struct{})
	for _, rt := range engine.Routes() {
		out[routeKey(rt.Method, rt.Path)] = struct{}{}
	}
	return out
}

func TestRegisterRestoreRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterRestoreRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	expected := []string{
		routeKey("POST", "/api/clusters/:namespace/:name/restore"),
		routeKey("POST", "/api/clusters/:namespace/:name/pitr"),
		routeKey("GET", "/api/clusters/:namespace/:name/restore-status"),
		routeKey("GET", "/api/restore-jobs"),
		routeKey("GET", "/api/restore-jobs/:namespace/:name"),
		routeKey("DELETE", "/api/restore-jobs/:namespace/:name"),
	}
	for _, k := range expected {
		assert.Contains(t, routes, k, "route should be registered: %s", k)
	}
}

func TestRegisterDiagnosticsRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterDiagnosticsRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	expected := []string{
		routeKey("POST", "/api/diagnostics/:namespace/:cluster/start"),
		routeKey("GET", "/api/diagnostics/:namespace/:id/status"),
		routeKey("GET", "/api/diagnostics/reports"),
		routeKey("GET", "/api/diagnostics/:namespace/:id/download"),
		routeKey("GET", "/api/diagnostics/:namespace/:id/file"),
		routeKey("DELETE", "/api/diagnostics/:namespace/:id"),
	}
	for _, k := range expected {
		assert.Contains(t, routes, k)
	}
}

func TestRegisterLogsRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterLogsRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	expected := []string{
		routeKey("GET", "/api/log-collectors"),
		routeKey("POST", "/api/log-collectors"),
		routeKey("GET", "/api/log-collectors/:namespace/:name"),
		routeKey("PUT", "/api/log-collectors/:namespace/:name"),
		routeKey("DELETE", "/api/log-collectors/:namespace/:name"),
		routeKey("GET", "/api/log-collectors/:namespace/pipeline"),
		routeKey("PUT", "/api/log-collectors/:namespace/pipeline"),
		routeKey("GET", "/api/log-collectors/:namespace/elastic-certs"),
		routeKey("PUT", "/api/log-collectors/:namespace/elastic-certs"),
		routeKey("GET", "/api/log-collectors/:namespace/:name/status"),
		routeKey("GET", "/api/log-collectors/:namespace/logstash/logs"),
		routeKey("POST", "/api/log-collectors/:namespace/test"),
		routeKey("GET", "/api/log-service/status"),
		routeKey("GET", "/api/log-strategies"),
		routeKey("POST", "/api/log-strategies"),
		routeKey("POST", "/api/log-strategies/precheck"),
		routeKey("GET", "/api/log-strategies/apply-records"),
		routeKey("GET", "/api/log-strategies/:name"),
		routeKey("PUT", "/api/log-strategies/:name"),
		routeKey("DELETE", "/api/log-strategies/:name"),
		routeKey("POST", "/api/log-strategies/:name/apply"),
		routeKey("POST", "/api/log-strategies/test-connection"),
		routeKey("POST", "/api/logs/bootstrap"),
		routeKey("GET", "/api/logs/bootstrap/status"),
		routeKey("GET", "/api/logs/bootstrap/logs"),
		routeKey("POST", "/api/logs/query"),
		routeKey("GET", "/api/logs/presets"),
		routeKey("GET", "/api/logs/presets/:pattern"),
	}
	for _, k := range expected {
		assert.Contains(t, routes, k)
	}
}

func TestRegisterMonitoringRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterMonitoringRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	expected := []string{
		routeKey("GET", "/api/monitors"),
		routeKey("POST", "/api/monitors"),
		routeKey("GET", "/api/monitors/:namespace/:name"),
		routeKey("PUT", "/api/monitors/:namespace/:name"),
		routeKey("DELETE", "/api/monitors/:namespace/:name"),
		routeKey("POST", "/api/monitoring/bootstrap"),
		routeKey("GET", "/api/monitoring/bootstrap/status"),
		routeKey("GET", "/api/monitoring/bootstrap/logs"),
		routeKey("GET", "/api/monitoring/status"),
		routeKey("GET", "/api/monitoring/preflight"),
		routeKey("DELETE", "/api/monitoring/uninstall"),
		routeKey("GET", "/api/monitoring/detect"),
		routeKey("POST", "/api/monitoring/plan"),
		routeKey("POST", "/api/monitoring/install"),
		routeKey("GET", "/api/monitoring/install/:sessionId/status"),
		routeKey("GET", "/api/monitoring/install/:sessionId/logs"),
		routeKey("POST", "/api/monitoring/install/:sessionId/retry"),
		routeKey("POST", "/api/monitoring/diagnose"),
		routeKey("POST", "/api/monitoring/auto-fix"),
		routeKey("GET", "/api/monitoring/grafana/config"),
		routeKey("PUT", "/api/monitoring/grafana/config"),
		routeKey("POST", "/api/monitoring/grafana/dashboards/sync"),
		routeKey("GET", "/api/monitoring/grafana/dashboards"),
		routeKey("GET", "/api/monitoring/grafana/dashboards/:name/versions"),
		routeKey("POST", "/api/monitoring/grafana/dashboards/:name/rollback"),
		routeKey("GET", "/api/monitoring/grafana/templates"),
		routeKey("GET", "/api/monitoring/grafana/templates/:name"),
		routeKey("GET", "/api/prometheus-rules"),
		routeKey("GET", "/api/prometheus-rules/:namespace/:name/yaml"),
		routeKey("POST", "/api/prometheus-rules/validate"),
		routeKey("GET", "/api/prometheus-rules/templates"),
		routeKey("GET", "/api/prometheus-rules/templates/:name"),
		routeKey("POST", "/api/prometheus-rules/apply"),
	}
	for _, k := range expected {
		assert.Contains(t, routes, k)
	}
}

func TestRegisterHealthRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterHealthRoutes(engine)

	routes := collectRoutes(engine)
	expected := []string{
		routeKey("GET", "/health"),
		routeKey("GET", "/healthz"),
		routeKey("GET", "/ready"),
		routeKey("GET", "/readyz"),
		routeKey("GET", "/version"),
	}
	for _, k := range expected {
		assert.Contains(t, routes, k)
	}
}

func TestRegisterPublicRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterPublicRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	// Check that public routes are registered
	assert.NotEmpty(t, routes)
}

func TestRegisterAuthRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterAuthRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	expected := []string{
		routeKey("POST", "/api/auth/login"),
		routeKey("GET", "/api/auth/me"),
	}
	for _, k := range expected {
		assert.Contains(t, routes, k)
	}
}

func TestRegisterClusterRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterClusterRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	expected := []string{
		routeKey("GET", "/api/clusters"),
		routeKey("POST", "/api/clusters"),
		routeKey("GET", "/api/clusters/:namespace/:name"),
		routeKey("PUT", "/api/clusters/:namespace/:name"),
		routeKey("DELETE", "/api/clusters/:namespace/:name"),
	}
	for _, k := range expected {
		assert.Contains(t, routes, k)
	}
}

func TestRegisterBackupRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterBackupRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	// Simply check routes are registered
	assert.NotEmpty(t, routes)
}

func TestRegisterXStoreRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterXStoreRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	// XStore routes include backups and followers
	expected := []string{
		routeKey("GET", "/api/xstore-backups"),
		routeKey("POST", "/api/xstore-backups"),
		routeKey("GET", "/api/xstore-followers"),
		routeKey("POST", "/api/xstore-followers"),
	}
	for _, k := range expected {
		assert.Contains(t, routes, k)
	}
}

func TestRegisterSystemRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterSystemRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	expected := []string{
		routeKey("GET", "/api/namespaces"),
		routeKey("GET", "/api/system-tasks"),
	}
	for _, k := range expected {
		assert.Contains(t, routes, k)
	}
}

func TestRegisterCRDAliasRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterCRDAliasRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	// CRD alias routes register additional paths
	assert.NotEmpty(t, routes)
}

func TestRegisterDomainRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterDomainRoutes(engine.Group("/api"))

	routes := collectRoutes(engine)
	// Domain routes use xstores path without domain prefix
	expected := []string{
		routeKey("GET", "/api/xstores"),
		routeKey("POST", "/api/xstores"),
		routeKey("GET", "/api/xstores/:namespace/:name"),
	}
	for _, k := range expected {
		assert.Contains(t, routes, k)
	}
}

func TestLogGroupedRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.GET("/test1", func(c *gin.Context) {})
	engine.POST("/test2", func(c *gin.Context) {})

	// LogGroupedRoutes should not panic
	assert.NotPanics(t, func() {
		LogGroupedRoutes(engine)
	})
}
