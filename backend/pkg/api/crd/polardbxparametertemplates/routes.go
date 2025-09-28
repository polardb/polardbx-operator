package polardbxparametertemplates

import (
	api_parameters "polardbx-ui-backend/pkg/api/parameters"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/polardbxparametertemplates")
	r.GET("", api_parameters.ListTemplates)
	r.POST("", api_parameters.CreateTemplate)
	item := r.Group("/:namespace/:name")
	item.GET("", api_parameters.GetTemplate)
	item.PUT("", api_parameters.UpdateTemplate)
	item.DELETE("", api_parameters.DeleteTemplate)
}
