package xstores

import (
	"polardbx-ui-backend/pkg/api/domain/xstores/services"

	"github.com/gin-gonic/gin"
)

func ListBackupBinlogs(c *gin.Context)  { services.NewBackupBinlogService().List(c) }
func CreateBackupBinlog(c *gin.Context) { services.NewBackupBinlogService().Create(c) }
func GetBackupBinlog(c *gin.Context)    { services.NewBackupBinlogService().Get(c) }
func UpdateBackupBinlog(c *gin.Context) { services.NewBackupBinlogService().Update(c) }
func DeleteBackupBinlog(c *gin.Context) { services.NewBackupBinlogService().Delete(c) }
