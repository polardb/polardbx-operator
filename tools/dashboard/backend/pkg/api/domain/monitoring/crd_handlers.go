package domain_monitoring

import (
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
	"polardbx-dashboard-backend/pkg/k8s"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
)

// ========================================
// PolarDBXMonitor CRD CRUD Handlers
// ========================================

// ListMonitors lists all PolarDBXMonitor resources
func ListMonitors(c *gin.Context) {
	k8sClient, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := util.DefaultNamespace(c, "default")
	monitors, err := k8s.ListPolarDBXMonitorsWithContext(c.Request.Context(), k8sClient, namespace)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, monitors)
}

// CreateMonitor creates a PolarDBXMonitor resource
func CreateMonitor(c *gin.Context) {
	k8sClient, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := util.DefaultNamespace(c, "default")
	var monitor polardbxv1.PolarDBXMonitor
	if err := c.ShouldBindJSON(&monitor); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	created, err := k8s.CreatePolarDBXMonitorWithContext(c.Request.Context(), k8sClient, namespace, &monitor)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, created)
}

// GetMonitor gets a PolarDBXMonitor resource
func GetMonitor(c *gin.Context) {
	k8sClient, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	m, err := k8s.GetPolarDBXMonitorWithContext(c.Request.Context(), k8sClient, ns, name)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, m)
}

// UpdateMonitor updates a PolarDBXMonitor resource
func UpdateMonitor(c *gin.Context) {
	k8sClient, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	var m polardbxv1.PolarDBXMonitor
	if err := c.ShouldBindJSON(&m); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	m.Namespace = ns
	um, err := k8s.UpdatePolarDBXMonitorWithContext(c.Request.Context(), k8sClient, ns, &m)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, um)
}

// DeleteMonitor deletes a PolarDBXMonitor resource
func DeleteMonitor(c *gin.Context) {
	k8sClient, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := k8s.DeletePolarDBXMonitorWithContext(c.Request.Context(), k8sClient, ns, name); err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "monitor deleted successfully"})
}
