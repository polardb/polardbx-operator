package clusterknobs

import (
	"net/http"

	"polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/k8s"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
)

// List
func GetList(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	list, err := k8s.GetClusterKnobsList(cli)
	if err != nil {
		util.HandleK8sError(c, "failed to get cluster knobs list", err)
		return
	}
	c.JSON(http.StatusOK, list)
}

// Create
func Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var payload polardbxv1.PolarDBXClusterKnobs
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cluster knobs data", "details": err.Error()})
		return
	}
	created, err := k8s.CreateClusterKnobs(cli, &payload)
	if err != nil {
		util.HandleK8sError(c, "failed to create cluster knobs", err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

// Get
func Get(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	name := c.Param("name")
	knobs, err := k8s.GetClusterKnobs(cli, namespace, name)
	if err != nil {
		util.HandleK8sError(c, "failed to get cluster knobs", err)
		return
	}
	c.JSON(http.StatusOK, knobs)
}

// Update
func Update(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	name := c.Param("name")
	var payload polardbxv1.PolarDBXClusterKnobs
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cluster knobs data", "details": err.Error()})
		return
	}
	payload.Namespace = namespace
	payload.Name = name
	updated, err := k8s.UpdateClusterKnobs(cli, &payload)
	if err != nil {
		util.HandleK8sError(c, "failed to update cluster knobs", err)
		return
	}
	c.JSON(http.StatusOK, updated)
}

// Delete
func Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	name := c.Param("name")
	if err := k8s.DeleteClusterKnobs(cli, namespace, name); err != nil {
		util.HandleK8sError(c, "failed to delete cluster knobs", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "cluster knobs deleted successfully"})
}
