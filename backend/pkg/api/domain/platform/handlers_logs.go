package platform

import (
	api_logs "polardbx-ui-backend/pkg/api/logs"
	api_logservice "polardbx-ui-backend/pkg/api/logservice"
	api_logstrategy "polardbx-ui-backend/pkg/api/logstrategy"

	"github.com/gin-gonic/gin"
)

func LogsQuery(c *gin.Context)           { api_logs.Query(c) }
func LogsPresets(c *gin.Context)         { api_logs.Presets(c) }
func LogsPresetByPattern(c *gin.Context) { api_logs.PresetByPattern(c) }

func LogServiceStatus(c *gin.Context) { api_logservice.Status(c) }

func LogStrategyList(c *gin.Context)     { api_logstrategy.List(c) }
func LogStrategyCreate(c *gin.Context)   { api_logstrategy.Create(c) }
func LogStrategyPrecheck(c *gin.Context) { api_logstrategy.Precheck(c) }
func LogStrategyGet(c *gin.Context)      { api_logstrategy.Get(c) }
func LogStrategyUpdate(c *gin.Context)   { api_logstrategy.Update(c) }
func LogStrategyDelete(c *gin.Context)   { api_logstrategy.Delete(c) }
func LogStrategyApply(c *gin.Context)    { api_logstrategy.Apply(c) }

func LogsBootstrap(c *gin.Context)      { api_logs.Bootstrap(c) }
func LogsBootstrapStatus(c *gin.Context) { api_logs.BootstrapStatus(c) }
func LogsBootstrapLogs(c *gin.Context)   { api_logs.BootstrapLogs(c) }
