package services

import (
	"net/http"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"

	"polardbx-ui-backend/pkg/api/domain/polardbxclusters/k8srepo"
	"polardbx-ui-backend/pkg/api/util"
)

// --- Cluster CRUD (行为保持不变) ---

func (s *ClusterService) List(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", "")
	ctx, cancel := util.ListCtx(c)
	defer cancel()
	clusters, err := k8srepo.NewClusterRepository().List(ctx, cli, ns)
	if err != nil {
		util.HandleK8sError(c, "failed to list clusters", err)
		return
	}
	c.JSON(http.StatusOK, clusters)
}

func (s *ClusterService) Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var obj polardbxv1.PolarDBXCluster
	if err := c.ShouldBindJSON(&obj); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to parse cluster data", "details": err.Error()})
		return
	}
	ns := obj.GetNamespace()
	if ns == "" {
		ns = "default"
	}
	ctx, cancel := util.CrudCtx(c)
	defer cancel()
	created, err := k8srepo.NewClusterRepository().Create(ctx, cli, ns, &obj)
	if err != nil {
		util.HandleK8sError(c, "failed to create cluster", err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

func (s *ClusterService) Get(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	ctx, cancel := util.ListCtx(c)
	defer cancel()
	cluster, err := k8srepo.NewClusterRepository().Get(ctx, cli, ns, name)
	if err != nil {
		util.HandleK8sError(c, "cluster not found", err)
		return
	}
	c.JSON(http.StatusOK, cluster)
}

func (s *ClusterService) Update(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	var body polardbxv1.PolarDBXCluster
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to parse cluster data", "details": err.Error()})
		return
	}
	ctx, cancel := util.CrudCtx(c)
	defer cancel()
	existing, err := k8srepo.NewClusterRepository().Get(ctx, cli, ns, name)
	if err != nil {
		util.HandleK8sError(c, "cluster not found", err)
		return
	}
	body.SetResourceVersion(existing.GetResourceVersion())
	updated, err := k8srepo.NewClusterRepository().Update(ctx, cli, ns, &body)
	if err != nil {
		util.HandleK8sError(c, "failed to update cluster", err)
		return
	}
	c.JSON(http.StatusOK, updated)
}

func (s *ClusterService) Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	ctx, cancel := util.CrudCtx(c)
	defer cancel()
	if err := k8srepo.NewClusterRepository().Delete(ctx, cli, ns, name); err != nil {
		util.HandleK8sError(c, "failed to delete cluster", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "cluster deletion initiated successfully"})
}
