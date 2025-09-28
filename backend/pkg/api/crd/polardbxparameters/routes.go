package polardbxparameters

import (
	api_parameters "polardbx-ui-backend/pkg/api/parameters"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/polardbxparameters")
	r.GET("", api_parameters.List)
	r.POST("", api_parameters.Create)
	r.GET("/:name", api_parameters.Get)
	r.PUT("/:name", api_parameters.Update)
	r.DELETE("/:name", api_parameters.Delete)
}
