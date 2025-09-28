package xstores

import (
	"polardbx-ui-backend/pkg/api/domain/xstores/services"

	"github.com/gin-gonic/gin"
)

func List(c *gin.Context)     { services.NewXStoreService().List(c) }
func Create(c *gin.Context)   { services.NewXStoreService().Create(c) }
func Get(c *gin.Context)      { services.NewXStoreService().Get(c) }
func Update(c *gin.Context)   { services.NewXStoreService().Update(c) }
func Delete(c *gin.Context)   { services.NewXStoreService().Delete(c) }
func ListPods(c *gin.Context) { services.NewXStoreService().ListPods(c) }
