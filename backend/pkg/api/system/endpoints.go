package system

import (
	"net/http"

	"polardbx-ui-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ContextInfo returns current k8s user, context and default namespace if provided.
func ContextInfo(c *gin.Context) {
	_, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	user, _ := c.Get("k8sUser")
	ctxName, _ := c.Get("k8sContext")
	defNS := c.DefaultQuery("namespace", "")
	if defNS == "" {
		if v, exists := c.Get("k8sDefaultNamespace"); exists {
			if s, ok := v.(string); ok {
				defNS = s
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{"user": user, "context": ctxName, "defaultNamespace": defNS})
}

// ListNamespaces returns all namespaces visible to the provided kubeconfig.
func ListNamespaces(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		// Return empty list to avoid frontend errors when kubeconfig is not initialized
		c.JSON(http.StatusOK, gin.H{"items": []any{}, "count": 0, "warning": "k8s client not initialized"})
		return
	}
	var nsList corev1.NamespaceList
	if err := cli.List(c.Request.Context(), &nsList, &client.ListOptions{}); err != nil {
		// Return empty list with details to keep UI working
		c.JSON(http.StatusOK, gin.H{"items": []any{}, "count": 0, "warning": "failed to list namespaces", "details": err.Error()})
		return
	}
	items := make([]gin.H, 0, len(nsList.Items))
	for _, ns := range nsList.Items {
		phase := string(ns.Status.Phase)
		items = append(items, gin.H{"name": ns.Name, "status": phase, "createdAt": ns.CreationTimestamp.Time})
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "count": len(items)})
}
