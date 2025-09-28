package platform

import (
	api_grafana "polardbx-ui-backend/pkg/api/grafana"

	"github.com/gin-gonic/gin"
)

func GrafanaGetConfig(c *gin.Context)      { api_grafana.GetConfig(c) }
func GrafanaPutConfig(c *gin.Context)      { api_grafana.PutConfig(c) }
func GrafanaSyncDashboards(c *gin.Context) { api_grafana.SyncDashboards(c) }
func GrafanaListDashboards(c *gin.Context) { api_grafana.ListDashboards(c) }
func GrafanaListVersions(c *gin.Context)   { api_grafana.ListDashboardVersions(c) }
func GrafanaRollback(c *gin.Context)       { api_grafana.RollbackDashboard(c) }
