package monitor

import (
	"net/http"

	"polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/k8s"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
)

func List(c *gin.Context) {
	k8sClient, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := util.DefaultNamespace(c, "default")
	monitors, err := k8s.ListPolarDBXMonitorsWithContext(c.Request.Context(), k8sClient, namespace)
	if err != nil {
		util.HandleK8sError(c, "failed to list monitors", err)
		return
	}
	c.JSON(http.StatusOK, monitors)
}

func Create(c *gin.Context) {
	k8sClient, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := util.DefaultNamespace(c, "default")
	var monitor polardbxv1.PolarDBXMonitor
	if err := c.ShouldBindJSON(&monitor); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to parse monitor data", "details": err.Error()})
		return
	}
	created, err := k8s.CreatePolarDBXMonitorWithContext(c.Request.Context(), k8sClient, namespace, &monitor)
	if err != nil {
		util.HandleK8sError(c, "failed to create monitor", err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

func Get(c *gin.Context) {
	k8sClient, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	m, err := k8s.GetPolarDBXMonitorWithContext(c.Request.Context(), k8sClient, ns, name)
	if err != nil {
		util.HandleK8sError(c, "failed to get monitor", err)
		return
	}
	c.JSON(http.StatusOK, m)
}

func Update(c *gin.Context) {
	k8sClient, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	var m polardbxv1.PolarDBXMonitor
	if err := c.ShouldBindJSON(&m); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to parse monitor data", "details": err.Error()})
		return
	}
	m.Namespace = ns
	um, err := k8s.UpdatePolarDBXMonitorWithContext(c.Request.Context(), k8sClient, ns, &m)
	if err != nil {
		util.HandleK8sError(c, "failed to update monitor", err)
		return
	}
	c.JSON(http.StatusOK, um)
}

func Delete(c *gin.Context) {
	k8sClient, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := k8s.DeletePolarDBXMonitorWithContext(c.Request.Context(), k8sClient, ns, name); err != nil {
		util.HandleK8sError(c, "failed to delete monitor", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "monitor deleted successfully"})
}
