package polardbxbackupschedules

import (
	"github.com/gin-gonic/gin"

	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"
	"polardbx-dashboard-backend/pkg/api/routerutil"
)

func RegisterRoutes(crd *gin.RouterGroup) {
	base := "/polardbxbackupschedules"
	routerutil.RegisterCRUDWithItemPattern(crd, base, base+"/:namespace/:name", routerutil.CRUDHandlers{
		List:   domain_pxc.ListSchedules,
		Create: domain_pxc.CreateSchedule,
		Get:    domain_pxc.GetSchedule,
		Update: domain_pxc.UpdateSchedule,
		Delete: domain_pxc.DeleteSchedule,
	})
}
