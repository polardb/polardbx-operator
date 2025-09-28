package polardbxlogcollectors

import (
	api_logcollector "polardbx-ui-backend/pkg/api/logcollector"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/polardbxlogcollectors")
	r.GET("", api_logcollector.List)
	r.POST("", api_logcollector.Create)
	item := r.Group("/:namespace/:name")
	item.GET("", api_logcollector.Get)
	item.PUT("", api_logcollector.Update)
	item.DELETE("", api_logcollector.Delete)
	// namespace-scoped extra endpoints
	ns := r.Group("/:namespace")
	ns.GET("/pipeline", api_logcollector.GetLogstashPipeline)
	ns.PUT("/pipeline", api_logcollector.UpdateLogstashPipeline)
	ns.GET("/elastic-certs", api_logcollector.GetElasticsearchCert)
	ns.PUT("/elastic-certs", api_logcollector.UpdateElasticsearchCert)
	ns.GET("/:name/status", api_logcollector.GetLogCollectorStatus)
	ns.GET("/logstash/logs", api_logcollector.StreamLogstashLogs)
	ns.POST("/test", api_logcollector.TestLogCollector)
}
