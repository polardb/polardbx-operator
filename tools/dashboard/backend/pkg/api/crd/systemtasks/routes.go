package systemtasks

import (
	domain_st "polardbx-dashboard-backend/pkg/api/domain/systemtasks"
	"polardbx-dashboard-backend/pkg/api/routerutil"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes adds /crd/systemtasks CRUD aliases.
func RegisterRoutes(crd *gin.RouterGroup) {
	base := "/systemtasks"
	routerutil.RegisterCRUDWithItemPattern(crd, base, base+"/:namespace/:name", routerutil.CRUDHandlers{
		List:   domain_st.List,
		Create: domain_st.Create,
		Get:    domain_st.Get,
		Update: domain_st.Update,
		Delete: domain_st.Delete,
	})
}
