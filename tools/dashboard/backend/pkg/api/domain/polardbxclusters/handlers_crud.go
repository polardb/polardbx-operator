package polardbxclusters

import (
	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/services"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/middleware"
	"polardbx-dashboard-backend/pkg/api/provider"
	"polardbx-dashboard-backend/pkg/api/util"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
)

// --- HTTP handlers: parameter binding + calling pure ClusterService methods ---

func svc(c *gin.Context) *services.ClusterService { return provider.Must(c).ClusterService(c) }

// List lists clusters in the given namespace.
// @Summary List PolarDB-X clusters
// @Description List PolarDB-X clusters in the specified namespace (or all when omitted).
// @Tags polardbxclusters
// @Produce json
// @Param namespace query string false "Kubernetes namespace filter"
// @Success 200 {array} map[string]any "List of clusters"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters [get]
func List(c *gin.Context) {
	logger := middleware.NewBusinessLogger(c, "ClusterService")
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		logger.Error(nil, "failed to get k8s client from context")
		return
	}
	ns := util.GetNamespace(c, "")
	logger.Info("listing clusters in namespace=%s", ns)

	ctx, cancel := util.ListCtx(c)
	defer cancel()

	clusters, err := svc(c).ListClusters(ctx, cli, ns)
	if err != nil {
		logger.Error(err, "failed to list clusters")
		middleware.LogK8sError(c, "List", "PolarDBXCluster", ns, "*", err)
		apierr.AbortWithError(c, err)
		return
	}
	logger.Info("listed %d clusters", len(clusters))
	apierr.OK(c, clusters)
}

// Create creates a new cluster.
// @Summary Create PolarDB-X cluster
// @Description Create a new PolarDB-X cluster from full CRD spec.
// @Tags polardbxclusters
// @Accept json
// @Produce json
// @Param body body map[string]any true "PolarDB-X cluster specification"
// @Success 201 {object} map[string]any "Created cluster"
// @Failure 400 {object} apierr.ErrorResponse "Invalid specification"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters [post]
func Create(c *gin.Context) {
	logger := middleware.NewBusinessLogger(c, "ClusterService")
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		logger.Error(nil, "failed to get k8s client from context")
		return
	}

	var obj polardbxv1.PolarDBXCluster
	ctx, cancel, ok := util.BindValidateAndCtx(c, &obj, util.DefaultCRUDTimeout)
	if !ok {
		return
	}
	defer cancel()

	ns := util.GetNamespace(c, obj.GetNamespace())
	if ns == "" {
		ns = "default"
	}
	logger.Info("creating cluster name=%s namespace=%s", obj.GetName(), ns)

	created, err := svc(c).CreateCluster(ctx, cli, ns, &obj)
	if err != nil {
		logger.Error(err, "failed to create cluster name=%s namespace=%s", obj.GetName(), ns)
		middleware.LogK8sError(c, "Create", "PolarDBXCluster", ns, obj.GetName(), err)
		middleware.LogAudit(c, "CREATE", "PolarDBXCluster", ns, obj.GetName(), false)
		apierr.AbortWithError(c, err)
		return
	}
	logger.Info("cluster created successfully name=%s namespace=%s", obj.GetName(), ns)
	middleware.LogAudit(c, "CREATE", "PolarDBXCluster", ns, obj.GetName(), true)
	apierr.Created(c, created)
}

// CreateFromConfig creates cluster from user-friendly configuration format.
// @Summary Create cluster from config
// @Description Create a new PolarDB-X cluster from user-friendly configuration payload.
// @Tags polardbxclusters
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param body body map[string]any true "Cluster creation config"
// @Success 201 {object} map[string]any "Created cluster"
// @Failure 400 {object} apierr.ErrorResponse "Invalid configuration"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/{namespace}/create [post]
func CreateFromConfig(c *gin.Context) {
	logger := middleware.NewBusinessLogger(c, "ClusterService")
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		logger.Error(nil, "failed to get k8s client from context")
		return
	}

	// Namespace from URL parameter, default to "default".
	ns := util.GetNamespace(c, "default")

	var config services.ClusterCreationConfig
	ctx, cancel, ok := util.BindValidateAndCtx(c, &config, util.DefaultCRUDTimeout)
	if !ok {
		return
	}
	defer cancel()

	logger.Info("creating cluster from config name=%s namespace=%s", config.Name, ns)

	created, err := svc(c).CreateClusterFromConfig(ctx, cli, ns, &config)
	if err != nil {
		logger.Error(err, "failed to create cluster from config name=%s namespace=%s", config.Name, ns)
		middleware.LogK8sError(c, "Create", "PolarDBXCluster", ns, config.Name, err)
		middleware.LogAudit(c, "CREATE", "PolarDBXCluster", ns, config.Name, false)
		apierr.AbortWithError(c, err)
		return
	}

	logger.Info("cluster created successfully from config name=%s namespace=%s", config.Name, ns)
	middleware.LogAudit(c, "CREATE", "PolarDBXCluster", ns, config.Name, true)
	apierr.Created(c, created)
}

// Get returns a single cluster.
// @Summary Get PolarDB-X cluster
// @Description Get a PolarDB-X cluster by namespace and name.
// @Tags polardbxclusters
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Cluster name"
// @Success 200 {object} map[string]any "Cluster"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/{namespace}/{name} [get]
func Get(c *gin.Context) {
	logger := middleware.NewBusinessLogger(c, "ClusterService")
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		logger.Error(nil, "failed to get k8s client from context")
		return
	}
	ns := util.GetNamespace(c, "default")
	name := c.Param("name")
	logger.Debug("getting cluster name=%s namespace=%s", name, ns)

	ctx, cancel := util.ListCtx(c)
	defer cancel()

	cluster, err := svc(c).GetCluster(ctx, cli, ns, name)
	if err != nil {
		logger.Error(err, "cluster not found name=%s namespace=%s", name, ns)
		middleware.LogK8sError(c, "Get", "PolarDBXCluster", ns, name, err)
		apierr.AbortWithError(c, err)
		return
	}
	logger.Debug("cluster retrieved successfully name=%s namespace=%s phase=%s",
		name, ns, cluster.Status.Phase)
	apierr.OK(c, cluster)
}

// Update updates an existing cluster.
// @Summary Update PolarDB-X cluster
// @Description Update an existing PolarDB-X cluster.
// @Tags polardbxclusters
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Cluster name"
// @Param body body map[string]any true "Updated cluster spec"
// @Success 200 {object} map[string]any "Updated cluster"
// @Failure 400 {object} apierr.ErrorResponse "Invalid specification"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/{namespace}/{name} [put]
func Update(c *gin.Context) {
	logger := middleware.NewBusinessLogger(c, "ClusterService")
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		logger.Error(nil, "failed to get k8s client from context")
		return
	}
	ns := util.GetNamespace(c, "default")
	name := c.Param("name")

	var body polardbxv1.PolarDBXCluster
	ctx, cancel, ok := util.BindValidateAndCtx(c, &body, util.DefaultCRUDTimeout)
	if !ok {
		return
	}
	defer cancel()

	logger.Info("updating cluster name=%s namespace=%s", name, ns)

	updated, err := svc(c).UpdateCluster(ctx, cli, ns, name, &body)
	if err != nil {
		logger.Error(err, "failed to update cluster name=%s namespace=%s", name, ns)
		middleware.LogK8sError(c, "Update", "PolarDBXCluster", ns, name, err)
		middleware.LogAudit(c, "UPDATE", "PolarDBXCluster", ns, name, false)
		apierr.AbortWithError(c, err)
		return
	}
	logger.Info("cluster updated successfully name=%s namespace=%s", name, ns)
	middleware.LogAudit(c, "UPDATE", "PolarDBXCluster", ns, name, true)
	apierr.OK(c, updated)
}

// Delete deletes a cluster.
// @Summary Delete PolarDB-X cluster
// @Description Delete a PolarDB-X cluster by namespace and name.
// @Tags polardbxclusters
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Cluster name"
// @Success 200 {object} map[string]any "Deletion initiated"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/{namespace}/{name} [delete]
func Delete(c *gin.Context) {
	logger := middleware.NewBusinessLogger(c, "ClusterService")
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		logger.Error(nil, "failed to get k8s client from context")
		return
	}
	ns := util.GetNamespace(c, "default")
	name := c.Param("name")
	logger.Info("deleting cluster name=%s namespace=%s", name, ns)

	ctx, cancel := util.CrudCtx(c)
	defer cancel()

	if err := svc(c).DeleteCluster(ctx, cli, ns, name); err != nil {
		logger.Error(err, "failed to delete cluster name=%s namespace=%s", name, ns)
		middleware.LogK8sError(c, "Delete", "PolarDBXCluster", ns, name, err)
		middleware.LogAudit(c, "DELETE", "PolarDBXCluster", ns, name, false)
		apierr.AbortWithError(c, err)
		return
	}
	logger.Info("cluster deletion initiated successfully name=%s namespace=%s", name, ns)
	middleware.LogAudit(c, "DELETE", "PolarDBXCluster", ns, name, true)
	apierr.OK(c, gin.H{"message": "cluster deletion initiated successfully"})
}
