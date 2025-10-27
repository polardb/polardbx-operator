package router

import (
	"github.com/gin-gonic/gin"

	crd_polardbxbackupbinlogs "polardbx-ui-backend/pkg/api/crd/polardbxbackupbinlogs"
	crd_polardbxbackups "polardbx-ui-backend/pkg/api/crd/polardbxbackups"
	crd_polardbxbackupschedules "polardbx-ui-backend/pkg/api/crd/polardbxbackupschedules"
	crd_polardbxclusters "polardbx-ui-backend/pkg/api/crd/polardbxclusters"
	crd_polardbxlogcollectors "polardbx-ui-backend/pkg/api/crd/polardbxlogcollectors"
	crd_polardbxmonitors "polardbx-ui-backend/pkg/api/crd/polardbxmonitors"
	crd_polardbxparameters "polardbx-ui-backend/pkg/api/crd/polardbxparameters"
	crd_polardbxparametertemplates "polardbx-ui-backend/pkg/api/crd/polardbxparametertemplates"
	crd_systemtasks "polardbx-ui-backend/pkg/api/crd/systemtasks"
	crd_xstorebackupbinlogs "polardbx-ui-backend/pkg/api/crd/xstorebackupbinlogs"
	crd_xstores "polardbx-ui-backend/pkg/api/crd/xstores"
	domain_platform "polardbx-ui-backend/pkg/api/domain/platform"

	// new imports for domain functions and logging
	"log"
	domain_polardbxclusters "polardbx-ui-backend/pkg/api/domain/polardbxclusters"
	domain_systemtasks "polardbx-ui-backend/pkg/api/domain/systemtasks"
	domain_xstores "polardbx-ui-backend/pkg/api/domain/xstores"
	"strings"
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

// RegisterDomainRoutes 在 /api/v1/* 下注册“领域入口”别名（不改变旧路由）。
// 注意：为避免与旧路由冲突，此处仅注册不会重复的前缀。
func RegisterDomainRoutes(v1 *gin.RouterGroup) {
	// 逻辑集群域：/api/v1/polardbxclusters （旧路由是 /api/v1/clusters）
	domain_polardbxclusters.RegisterRoutes(v1)

	// 平台任务域：/api/v1/systemtasks （旧路由是 /api/v1/system-tasks）
	domain_systemtasks.RegisterRoutes(v1)

	// 存储域：/api/v1/xstores （与旧路由并存提供域入口别名）
	domain_xstores.RegisterRoutes(v1)
}

// LogGroupedRoutes 将所有已注册路由按领域分组输出概要日志，便于发现性。
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

	log.Printf("Route groups: polardbxclusters=%d xstores=%d systemtasks=%d platform=%d crd=%d others=%d",
		counters["polardbxclusters"], counters["xstores"], counters["systemtasks"], counters["platform"], counters["crd"], counters["others"],
	)
}
