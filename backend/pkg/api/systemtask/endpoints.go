package systemtask

import (
	"net/http"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"

	"polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/k8s"
)

func List(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	items, err := k8s.ListSystemTasksWithContext(c.Request.Context(), cli, ns)
	if err != nil {
		util.HandleK8sError(c, "failed to list system tasks", err)
		return
	}
	c.JSON(http.StatusOK, items)
}

func Get(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	item, err := k8s.GetSystemTaskWithContext(c.Request.Context(), cli, ns, name)
	if err != nil {
		util.HandleK8sError(c, "failed to get system task", err)
		return
	}
	c.JSON(http.StatusOK, item)
}

func Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.SystemTask
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid system task", "details": err.Error()})
		return
	}
	created, err := k8s.CreateSystemTaskWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		util.HandleK8sError(c, "failed to create system task", err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

func Update(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	var body polardbxv1.SystemTask
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid system task", "details": err.Error()})
		return
	}
	updated, err := k8s.UpdateSystemTaskWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		util.HandleK8sError(c, "failed to update system task", err)
		return
	}
	c.JSON(http.StatusOK, updated)
}

func Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := k8s.DeleteSystemTaskWithContext(c.Request.Context(), cli, ns, name); err != nil {
		util.HandleK8sError(c, "failed to delete system task", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "system task deleted"})
}
