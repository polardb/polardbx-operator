package xstores

import (
	domain_xs "polardbx-dashboard-backend/pkg/api/domain/xstores"
	"polardbx-dashboard-backend/pkg/api/routerutil"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes adds /crd/xstores CRUD aliases.
func RegisterRoutes(crd *gin.RouterGroup) {
	base := "/xstores"
	routerutil.RegisterCRUDWithItemPattern(crd, base, base+"/:namespace/:name", routerutil.CRUDHandlers{
		List:   domain_xs.List,
		Create: domain_xs.Create,
		Get:    domain_xs.Get,
		Update: domain_xs.Update,
		Delete: domain_xs.Delete,
	})
	item := crd.Group(base + "/:namespace/:name")
	item.GET("/pods", domain_xs.ListPods)
}
