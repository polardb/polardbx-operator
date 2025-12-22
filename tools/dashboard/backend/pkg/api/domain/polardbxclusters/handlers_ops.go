package polardbxclusters

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-dashboard-backend/pkg/api/domain/platform/pod/handler"
	domain_prechange "polardbx-dashboard-backend/pkg/api/domain/platform/prechange/handler"
	domain_restore "polardbx-dashboard-backend/pkg/api/domain/platform/restore/handler"
	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/services"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

// --- Thin handlers forwarding to services/others ---

// UpdateLogConfig updates log configuration for a specific node type of a cluster.
// @Summary Update cluster log config
// @Description Update log configuration for a given node type of a PolarDB-X cluster.
// @Tags polardbxclusters, ops
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Cluster name"
// @Param nodeType path string true "Node type (e.g. cn, dn, gms)"
// @Param body body map[string]any true "Log config payload"
// @Success 200 {object} map[string]any "Update confirmation"
// @Failure 400 {object} apierr.ErrorResponse "Invalid payload"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/log-config/{nodeType} [patch]
func UpdateLogConfig(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	nodeType := c.Param("nodeType")
	var req services.LogConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if err := services.NewClusterService().UpdateLogConfig(c.Request.Context(), cli, ns, name, nodeType, &req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": nodeType + " log config updated successfully"})
}

// Scale performs scaling operations for a cluster.
// @Summary Scale cluster
// @Description Initiate scaling operation (e.g. change node replicas) for a PolarDB-X cluster.
// @Tags polardbxclusters, ops
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Cluster name"
// @Param body body map[string]any true "Scaling request"
// @Success 200 {object} map[string]any "Scaling initiated"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/scale [patch]
func Scale(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	var req services.ClusterScalingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if err := services.NewClusterService().Scale(c.Request.Context(), cli, ns, name, &req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "Cluster scaling initiated successfully"})
}

// Upgrade initiates an upgrade for a cluster.
// @Summary Upgrade cluster
// @Description Initiate version upgrade for a PolarDB-X cluster.
// @Tags polardbxclusters, ops
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Cluster name"
// @Param body body map[string]any true "Upgrade request"
// @Success 200 {object} map[string]any "Upgrade initiated"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/upgrade [patch]
func Upgrade(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	var req services.ClusterUpgradeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if err := services.NewClusterService().Upgrade(c.Request.Context(), cli, ns, name, &req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "Cluster upgrade initiated successfully", "upgrade": gin.H{"targetVersion": req.TargetVersion, "strategy": req.Strategy, "status": "upgrade initiated"}})
}

// GetAlertsSummary gets alert summary for a cluster.
// @Summary Get cluster alerts summary
// @Description Get aggregated alert summary for the specified PolarDB-X cluster.
// @Tags polardbxclusters, monitoring
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Cluster name"
// @Success 200 {object} map[string]any "Alerts summary"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 502 {object} apierr.ErrorResponse "Backend error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/alerts-summary [get]
func GetAlertsSummary(c *gin.Context) { services.GetAlertsSummary(c) }

// ListPods lists pods for a cluster.
// @Summary List cluster pods
// @Description List pods belonging to the specified PolarDB-X cluster.
// @Tags polardbxclusters, pods
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Cluster name"
// @Success 200 {array} map[string]any "List of pods"
// @Failure 404 {object} apierr.ErrorResponse "Cluster or pods not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/pods [get]
func ListPods(c *gin.Context) { handler.ListForCluster(c) }

func GetPrechangeChecklist(c *gin.Context) { domain_prechange.GetPrechangeChecklist(c) }
func Precheck(c *gin.Context)              { domain_prechange.Precheck(c) }

func RestoreCluster(c *gin.Context)   { domain_restore.RestoreCluster(c) }
func InitiatePITR(c *gin.Context)     { domain_restore.InitiatePITR(c) }
func GetRestoreStatus(c *gin.Context) { domain_restore.GetRestoreStatus(c) }

// UpgradeCandidate upgrade candidate version
type UpgradeCandidate struct {
	Version     string `json:"version"`
	Recommended bool   `json:"recommended,omitempty"`
	Notes       string `json:"notes,omitempty"`
}

// UpgradePlanResponse upgrade plan response
type UpgradePlanResponse struct {
	CurrentVersion string             `json:"currentVersion"`
	Candidates     []UpgradeCandidate `json:"candidates"`
	Matrix         map[string]any     `json:"matrix,omitempty"`
}

// GetUpgradePlan gets cluster upgrade plan (candidate version list).
// @Summary Get cluster upgrade plan
// @Description Get upgrade plan and candidate versions for a PolarDB-X cluster.
// @Tags polardbxclusters, ops
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Cluster name"
// @Success 200 {object} polardbxclusters.UpgradePlanResponse "Upgrade plan"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/upgrade-plan [get]
func GetUpgradePlan(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	namespace := c.Param("namespace")
	name := c.Param("name")

	// Get current cluster information
	var cluster polardbxv1.PolarDBXCluster
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: namespace, Name: name}, &cluster); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	currentVersion := cluster.Spec.Topology.Version
	if currentVersion == "" {
		currentVersion = "unknown"
	}

	// Build candidate version list
	// In production environment, available versions can be obtained from ConfigMap or external service
	candidates := buildUpgradeCandidates(currentVersion)

	resp := UpgradePlanResponse{
		CurrentVersion: currentVersion,
		Candidates:     candidates,
		Matrix: map[string]any{
			"minVersion": "5.4.13",
			"maxVersion": "5.4.19",
		},
	}

	apierr.OK(c, resp)
}

// buildUpgradeCandidates builds upgrade candidate list
func buildUpgradeCandidates(currentVersion string) []UpgradeCandidate {
	// Predefined version list (can be read from ConfigMap in practice)
	allVersions := []string{"5.4.19", "5.4.18", "5.4.17", "5.4.16", "5.4.15", "5.4.14", "5.4.13"}

	var candidates []UpgradeCandidate
	for i, v := range allVersions {
		// Skip current version and lower versions
		if v == currentVersion {
			break
		}
		candidate := UpgradeCandidate{
			Version: v,
		}
		if i == 0 {
			candidate.Recommended = true
			candidate.Notes = "Latest stable version"
		}
		candidates = append(candidates, candidate)
	}

	// If no higher versions, return default candidates
	if len(candidates) == 0 {
		candidates = []UpgradeCandidate{
			{Version: "5.4.19", Recommended: true, Notes: "Latest stable version"},
			{Version: "5.4.18"},
		}
	}

	return candidates
}
