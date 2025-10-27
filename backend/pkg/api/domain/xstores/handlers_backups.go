package xstores

import (
	"polardbx-ui-backend/pkg/api/domain/xstores/services"

	"github.com/gin-gonic/gin"
)

func ListBackups(c *gin.Context)         { services.NewBackupsService().List(c) }
func CreateBackup(c *gin.Context)        { services.NewBackupsService().Create(c) }
func GetBackup(c *gin.Context)           { services.NewBackupsService().Get(c) }
func UpdateBackup(c *gin.Context)        { services.NewBackupsService().Update(c) }
func DeleteBackup(c *gin.Context)        { services.NewBackupsService().Delete(c) }
func ForceDeleteBackup(c *gin.Context)   { services.NewBackupsService().ForceDelete(c) }
func GetBackupRemoteInfo(c *gin.Context) { services.NewBackupsService().RemoteInfo(c) }
