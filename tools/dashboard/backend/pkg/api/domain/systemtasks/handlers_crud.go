package systemtasks

import (
	"polardbx-dashboard-backend/pkg/api/domain/systemtasks/handler"
	"polardbx-dashboard-backend/pkg/api/domain/systemtasks/repository"
	"polardbx-dashboard-backend/pkg/api/domain/systemtasks/service"

	"github.com/gin-gonic/gin"
)

// Default Handler instance (using dependency injection)
var defaultHandler = handler.NewSystemTaskHandler(
	service.NewSystemTaskService(
		repository.NewK8sSystemTaskRepository(),
	),
)

// The following functions maintain backward compatibility and delegate to Handler

func List(c *gin.Context)   { defaultHandler.List(c) }
func Create(c *gin.Context) { defaultHandler.Create(c) }
func Get(c *gin.Context)    { defaultHandler.Get(c) }
func Update(c *gin.Context) { defaultHandler.Update(c) }
func Delete(c *gin.Context) { defaultHandler.Delete(c) }
