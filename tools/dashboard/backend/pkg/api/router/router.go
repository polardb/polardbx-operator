package router

import (
	"strings"

	"github.com/gin-gonic/gin"

	crd_polardbxbackupbinlogs "polardbx-dashboard-backend/pkg/api/crd/polardbxbackupbinlogs"
	crd_polardbxbackups "polardbx-dashboard-backend/pkg/api/crd/polardbxbackups"
	crd_polardbxbackupschedules "polardbx-dashboard-backend/pkg/api/crd/polardbxbackupschedules"
	crd_polardbxclusters "polardbx-dashboard-backend/pkg/api/crd/polardbxclusters"
	crd_polardbxlogcollectors "polardbx-dashboard-backend/pkg/api/crd/polardbxlogcollectors"
	crd_polardbxmonitors "polardbx-dashboard-backend/pkg/api/crd/polardbxmonitors"
	crd_polardbxparameters "polardbx-dashboard-backend/pkg/api/crd/polardbxparameters"
	crd_polardbxparametertemplates "polardbx-dashboard-backend/pkg/api/crd/polardbxparametertemplates"
	crd_systemtasks "polardbx-dashboard-backend/pkg/api/crd/systemtasks"
	crd_xstorebackupbinlogs "polardbx-dashboard-backend/pkg/api/crd/xstorebackupbinlogs"
	crd_xstores "polardbx-dashboard-backend/pkg/api/crd/xstores"
	domain_platform "polardbx-dashboard-backend/pkg/api/domain/platform"
	domain_polardbxclusters "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"
	domain_systemtasks "polardbx-dashboard-backend/pkg/api/domain/systemtasks"
	domain_xstores "polardbx-dashboard-backend/pkg/api/domain/xstores"
	"polardbx-dashboard-backend/pkg/logger"
)

// RegisterCRDAliasRoutes registers /api/v1/crd/* alias routes that forward to existing handlers.
// This does not change existing endpoints; it only adds discoverable CRD-aligned paths.
func RegisterCRDAliasRoutes(v1 *gin.RouterGroup) {
	crd := v1.Group("/crd")
	crd_polardbxclusters.RegisterRoutes(crd)
	crd_xstores.RegisterRoutes(crd)
	crd_systemtasks.RegisterRoutes(crd)
	crd_polardbxbackups.RegisterRoutes(crd)
	crd_polardbxbackupschedules.RegisterRoutes(crd)
	crd_polardbxbackupbinlogs.RegisterRoutes(crd)
	crd_xstorebackupbinlogs.RegisterRoutes(crd)
	crd_polardbxparameters.RegisterRoutes(crd)
	crd_polardbxparametertemplates.RegisterRoutes(crd)
	crd_polardbxmonitors.RegisterRoutes(crd)
	crd_polardbxlogcollectors.RegisterRoutes(crd)

	// Platform domain aliases under /api/v1/platform
	domain_platform.RegisterRoutes(v1)
}

// RegisterDomainRoutes registers "domain entry" aliases under /api/v1/* (without changing old routes).
// Note: To avoid conflicts with old routes, only non-duplicate prefixes are registered here.
func RegisterDomainRoutes(v1 *gin.RouterGroup) {
	// Logical cluster domain: /api/v1/polardbxclusters (old route is /api/v1/clusters)
	domain_polardbxclusters.RegisterRoutes(v1)

	// Platform task domain: /api/v1/systemtasks (old route is /api/v1/system-tasks)
	domain_systemtasks.RegisterRoutes(v1)

	// Storage domain: /api/v1/xstores (coexists with old route to provide domain entry alias)
	domain_xstores.RegisterRoutes(v1)
}

// LogGroupedRoutes outputs summary logs of all registered routes grouped by domain for discoverability.
func LogGroupedRoutes(engine *gin.Engine) {
	counters := map[string]int{
		"polardbxclusters": 0,
		"xstores":          0,
		"systemtasks":      0,
		"platform":         0,
		"crd":              0,
		"others":           0,
	}

	for _, rt := range engine.Routes() {
		path := rt.Path
		group := "others"
		if strings.HasPrefix(path, "/api/v1/crd/") {
			group = "crd"
		} else if strings.HasPrefix(path, "/api/v1/polardbxclusters") ||
			strings.HasPrefix(path, "/api/v1/clusters") ||
			strings.HasPrefix(path, "/api/v1/backups") ||
			strings.HasPrefix(path, "/api/v1/backup-schedules") ||
			strings.HasPrefix(path, "/api/v1/backup-binlogs") ||
			strings.HasPrefix(path, "/api/v1/parameters") ||
			strings.HasPrefix(path, "/api/v1/parameter-templates") ||
			strings.HasPrefix(path, "/api/v1/cluster-knobs") ||
			strings.HasPrefix(path, "/api/v1/prechange") ||
			strings.HasPrefix(path, "/api/v1/restore") {
			group = "polardbxclusters"
		} else if strings.HasPrefix(path, "/api/v1/xstores") ||
			strings.HasPrefix(path, "/api/v1/xstore-backups") ||
			strings.HasPrefix(path, "/api/v1/xstore-followers") ||
			strings.HasPrefix(path, "/api/v1/xstore-rebuild") {
			group = "xstores"
		} else if strings.HasPrefix(path, "/api/v1/systemtasks") ||
			strings.HasPrefix(path, "/api/v1/system-tasks") {
			group = "systemtasks"
		} else if strings.HasPrefix(path, "/api/v1/platform") ||
			strings.HasPrefix(path, "/api/v1/monitoring") ||
			strings.HasPrefix(path, "/api/v1/grafana") ||
			strings.HasPrefix(path, "/api/v1/logs") ||
			strings.HasPrefix(path, "/api/v1/log-service") ||
			strings.HasPrefix(path, "/api/v1/log-strategies") ||
			strings.HasPrefix(path, "/api/v1/system") ||
			strings.HasPrefix(path, "/api/v1/pods") ||
			strings.HasPrefix(path, "/api/v1/pod") ||
			strings.HasPrefix(path, "/api/v1/alerts") ||
			strings.HasPrefix(path, "/api/v1/settings") ||
			strings.HasPrefix(path, "/api/v1/auth") {
			group = "platform"
		}
		counters[group]++
	}

	logger.Info("Route groups registered",
		"polardbxclusters", counters["polardbxclusters"],
		"xstores", counters["xstores"],
		"systemtasks", counters["systemtasks"],
		"platform", counters["platform"],
		"crd", counters["crd"],
		"others", counters["others"],
	)
}

// Note: SetupRouter is now defined in setup.go to avoid duplicate declaration.
// This file contains route registration helpers.
