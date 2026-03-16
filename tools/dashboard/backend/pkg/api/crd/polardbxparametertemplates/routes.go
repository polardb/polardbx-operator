package polardbxparametertemplates

import (
	domain_parameters "polardbx-dashboard-backend/pkg/api/domain/platform/parameters/handler"
	"polardbx-dashboard-backend/pkg/api/routerutil"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(crd *gin.RouterGroup) {
	base := "/polardbxparametertemplates"
	routerutil.RegisterCRUDWithItemPattern(crd, base, base+"/:namespace/:name", routerutil.CRUDHandlers{
		List:   domain_parameters.ListTemplates,
		Create: domain_parameters.CreateTemplate,
		Get:    domain_parameters.GetTemplate,
		Update: domain_parameters.UpdateTemplate,
		Delete: domain_parameters.DeleteTemplate,
	})
}
