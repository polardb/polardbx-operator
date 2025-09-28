package platform

import (
	api_settings "polardbx-ui-backend/pkg/api/settings"

	"github.com/gin-gonic/gin"
)

func GetBackupDashboard(c *gin.Context) { api_settings.Get(c) }
func PutBackupDashboard(c *gin.Context) { api_settings.Update(c) }
