package xstores

import (
	domain_xs "polardbx-ui-backend/pkg/api/domain/xstores"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes adds /crd/xstores CRUD aliases.
func RegisterRoutes(crd *gin.RouterGroup) {
	r := crd.Group("/xstores")
	r.GET("", domain_xs.List)
	r.POST("", domain_xs.Create)
	item := r.Group("/:namespace/:name")
	item.GET("", domain_xs.Get)
	item.PUT("", domain_xs.Update)
	item.DELETE("", domain_xs.Delete)
	item.GET("/pods", domain_xs.ListPods)
}
