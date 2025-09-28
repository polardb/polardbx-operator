package platform

import (
	api_alerts "polardbx-ui-backend/pkg/api/alerts"

	"github.com/gin-gonic/gin"
)

func AlertsList(c *gin.Context)          { api_alerts.List(c) }
func AlertsListProfiles(c *gin.Context)  { api_alerts.ListProfiles(c) }
func AlertsCreateProfile(c *gin.Context) { api_alerts.CreateProfile(c) }
func AlertsGetProfile(c *gin.Context)    { api_alerts.GetProfile(c) }
func AlertsUpdateProfile(c *gin.Context) { api_alerts.UpdateProfile(c) }
func AlertsDeleteProfile(c *gin.Context) { api_alerts.DeleteProfile(c) }
func AlertsDryRunProfile(c *gin.Context) { api_alerts.DryRunProfile(c) }
func AlertsGetRoutes(c *gin.Context)     { api_alerts.GetRoutes(c) }
func AlertsPutRoutes(c *gin.Context)     { api_alerts.PutRoutes(c) }
func AlertsListSilences(c *gin.Context)  { api_alerts.ListSilences(c) }
func AlertsCreateSilence(c *gin.Context) { api_alerts.CreateSilence(c) }
func AlertsDeleteSilence(c *gin.Context) { api_alerts.DeleteSilence(c) }
func AlertsTest(c *gin.Context)          { api_alerts.TestAlert(c) }
