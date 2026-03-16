package polardbxclusters

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"

	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/services"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

// --- Handlers forwarding to BackupBinlog service (pure business methods) ---

// ListBackupBinlogs lists backup binlog CRs in the specified namespace.
// @Summary List backup binlogs
// @Description List PolarDB-X backup binlog resources, optionally filtered by namespace.
// @Tags polardbxclusters, backup-binlogs
// @Produce json
// @Param namespace query string false "Kubernetes namespace filter"
// @Success 200 {array} map[string]any "List of backup binlogs"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/backup-binlogs [get]
func ListBackupBinlogs(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", "")
	items, err := services.NewBackupBinlogService().ListBinlogs(c.Request.Context(), cli, ns)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, items)
}

// CreateBackupBinlog creates a new backup binlog CR.
// @Summary Create backup binlog
// @Description Create a new PolarDB-X backup binlog resource.
// @Tags polardbxclusters, backup-binlogs
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Param body body map[string]any true "Backup binlog specification"
// @Success 201 {object} map[string]any "Created backup binlog"
// @Failure 400 {object} apierr.ErrorResponse "Invalid specification"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/backup-binlogs [post]
func CreateBackupBinlog(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", "default")
	var obj polardbxv1.PolarDBXBackupBinlog
	if err := c.ShouldBindJSON(&obj); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	created, err := services.NewBackupBinlogService().CreateBinlog(c.Request.Context(), cli, ns, &obj)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, created)
}

// GetBackupBinlog gets a single backup binlog by namespace and name.
// @Summary Get backup binlog
// @Description Get a PolarDB-X backup binlog resource by namespace and name.
// @Tags polardbxclusters, backup-binlogs
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Backup binlog name"
// @Success 200 {object} map[string]any "Backup binlog"
// @Failure 404 {object} apierr.ErrorResponse "Backup binlog not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/backup-binlogs/{namespace}/{name} [get]
func GetBackupBinlog(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	obj, err := services.NewBackupBinlogService().GetBinlog(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, obj)
}

// UpdateBackupBinlog updates an existing backup binlog.
// @Summary Update backup binlog
// @Description Update an existing PolarDB-X backup binlog resource.
// @Tags polardbxclusters, backup-binlogs
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Backup binlog name"
// @Param body body map[string]any true "Updated backup binlog specification"
// @Success 200 {object} map[string]any "Updated backup binlog"
// @Failure 400 {object} apierr.ErrorResponse "Invalid specification"
// @Failure 404 {object} apierr.ErrorResponse "Backup binlog not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/backup-binlogs/{namespace}/{name} [put]
func UpdateBackupBinlog(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	var obj polardbxv1.PolarDBXBackupBinlog
	if err := c.ShouldBindJSON(&obj); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	updated, err := services.NewBackupBinlogService().UpdateBinlog(c.Request.Context(), cli, ns, &obj)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, updated)
}

// DeleteBackupBinlog deletes a backup binlog.
// @Summary Delete backup binlog
// @Description Delete a PolarDB-X backup binlog resource by namespace and name.
// @Tags polardbxclusters, backup-binlogs
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Backup binlog name"
// @Success 200 {object} map[string]any "Deletion confirmation"
// @Failure 404 {object} apierr.ErrorResponse "Backup binlog not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/polardbxclusters/backup-binlogs/{namespace}/{name} [delete]
func DeleteBackupBinlog(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := services.NewBackupBinlogService().DeleteBinlog(c.Request.Context(), cli, ns, name); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "backup binlog deleted"})
}
