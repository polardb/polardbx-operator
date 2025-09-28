package systemtasks

import (
	api_systemtask "polardbx-ui-backend/pkg/api/systemtask"

	"github.com/gin-gonic/gin"
)

func List(c *gin.Context)   { api_systemtask.List(c) }
func Create(c *gin.Context) { api_systemtask.Create(c) }
func Get(c *gin.Context)    { api_systemtask.Get(c) }
func Update(c *gin.Context) { api_systemtask.Update(c) }
func Delete(c *gin.Context) { api_systemtask.Delete(c) }
