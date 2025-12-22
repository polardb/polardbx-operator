package polardbxclusters

import (
	"errors"

	"github.com/gin-gonic/gin"

	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/services"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

// GetResourceUsage returns aggregated CPU/memory usage for a cluster (engine/server only).
// Data source: metrics-server (metrics.k8s.io). If metrics-server is not installed, returns available=false.
// @Summary Get cluster resource usage
// @Description Aggregated CPU (cores, % of requests/limits) and memory (bytes/GiB, % of requests/limits) for engine/server containers.
// @Tags polardbxclusters, monitoring
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Cluster name"
// @Success 200 {object} services.ClusterResourceUsage "Resource usage snapshot"
// @Failure 403 {object} apierr.ErrorResponse "Forbidden (missing metrics RBAC)"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/resource-usage [get]
func GetResourceUsage(c *gin.Context) {
	cli, _, dyn, ok := util.GetK8sClients(c)
	if !ok || cli == nil || dyn == nil {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	ctx, cancel := util.ListCtx(c)
	defer cancel()

	usage, err := services.GetClusterResourceUsage(ctx, cli, dyn, ns, name)
	if err != nil {
		// metrics-server absent: return non-error response to let UI render empty state.
		if errors.Is(err, services.ErrMetricsNotAvailable) {
			apierr.OK(c, usage)
			return
		}
		apierr.AbortK8sError(c, "get cluster resource usage", err)
		return
	}
	apierr.OK(c, usage)
}
