package logcollector

import (
	"net/http"

	"polardbx-ui-backend/pkg/k8s"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-ui-backend/pkg/api/util"
)

func k8sClientFromContext(c *gin.Context) (client.Client, bool) {
	v, ok := c.Get("k8sClient")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "kubernetes client not initialized"})
		return nil, false
	}
	cli, ok := v.(client.Client)
	if !ok || cli == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid kubernetes client in context"})
		return nil, false
	}
	return cli, true
}

func List(c *gin.Context) {
	k8sClient, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.DefaultQuery("namespace", "default")
	collectors, err := k8s.ListPolarDBXLogCollectorsWithContext(c.Request.Context(), k8sClient, namespace)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list log collectors", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, collectors)
}

func Create(c *gin.Context) {
	k8sClient, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.DefaultQuery("namespace", "default")
	var collector polardbxv1.PolarDBXLogCollector
	if err := c.ShouldBindJSON(&collector); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to parse log collector data", "details": err.Error()})
		return
	}
	createdCollector, err := k8s.CreatePolarDBXLogCollectorWithContext(c.Request.Context(), k8sClient, namespace, &collector)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create log collector", "details": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, createdCollector)
}

func Get(c *gin.Context) {
	k8sClient, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	name := c.Param("name")
	collector, err := k8s.GetPolarDBXLogCollectorWithContext(c.Request.Context(), k8sClient, namespace, name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "failed to get log collector", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, collector)
}

func Update(c *gin.Context) {
	k8sClient, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	var collector polardbxv1.PolarDBXLogCollector
	if err := c.ShouldBindJSON(&collector); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to parse log collector data", "details": err.Error()})
		return
	}
	collector.Namespace = namespace
	updatedCollector, err := k8s.UpdatePolarDBXLogCollectorWithContext(c.Request.Context(), k8sClient, namespace, &collector)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update log collector", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, updatedCollector)
}

func Delete(c *gin.Context) {
	k8sClient, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	name := c.Param("name")
	if err := k8s.DeletePolarDBXLogCollectorWithContext(c.Request.Context(), k8sClient, namespace, name); err != nil {
		util.HandleK8sError(c, "failed to delete log collector", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "log collector deleted successfully"})
}
