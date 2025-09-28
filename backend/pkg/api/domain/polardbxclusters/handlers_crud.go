package polardbxclusters

import (
	"polardbx-ui-backend/pkg/api/domain/polardbxclusters/services"

	"github.com/gin-gonic/gin"
)

// --- Thin handlers forwarding to services ---

func List(c *gin.Context)             { services.NewClusterService().List(c) }
func Create(c *gin.Context)           { services.NewClusterService().Create(c) }
func CreateFromConfig(c *gin.Context) { services.NewClusterService().Create(c) }
func Get(c *gin.Context)              { services.NewClusterService().Get(c) }
func Update(c *gin.Context)           { services.NewClusterService().Update(c) }
func Delete(c *gin.Context)           { services.NewClusterService().Delete(c) }
