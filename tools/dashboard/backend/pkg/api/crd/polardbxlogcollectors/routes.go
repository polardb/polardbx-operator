package polardbxlogcollectors

import (
	domain_logcollector "polardbx-dashboard-backend/pkg/api/domain/platform/logcollector/handler"
	"polardbx-dashboard-backend/pkg/api/routerutil"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(crd *gin.RouterGroup) {
	base := "/polardbxlogcollectors"
	routerutil.RegisterCRUDWithItemPattern(crd, base, base+"/:namespace/:name", routerutil.CRUDHandlers{
		List:   domain_logcollector.List,
		Create: domain_logcollector.Create,
		Get:    domain_logcollector.Get,
		Update: domain_logcollector.Update,
		Delete: domain_logcollector.Delete,
	})
	// namespace-scoped extra endpoints
	ns := crd.Group(base + "/:namespace")
	ns.GET("/pipeline", domain_logcollector.GetLogstashPipeline)
	ns.PUT("/pipeline", domain_logcollector.UpdateLogstashPipeline)
	ns.GET("/elastic-certs", domain_logcollector.GetElasticsearchCert)
	ns.PUT("/elastic-certs", domain_logcollector.UpdateElasticsearchCert)
	ns.GET("/:name/status", domain_logcollector.GetLogCollectorStatus)
	ns.GET("/logstash/logs", domain_logcollector.StreamLogstashLogs)
	ns.POST("/test", domain_logcollector.TestLogCollector)
}
