package xstores

import (
	"polardbx-dashboard-backend/pkg/api/domain/xstores/services"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/provider"
	"polardbx-dashboard-backend/pkg/api/util"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
)

// backupBinlogSvc gets BackupBinlogService from Provider
func backupBinlogSvc(c *gin.Context) *services.BackupBinlogService {
	return provider.Must(c).BackupBinlogService(c)
}

// ListBackupBinlogs lists binlog backups for XStores.
// @Summary List XStore binlog backups
// @Description List binlog backups for XStore instances in the specified or default namespace.
// @Tags xstores, backup-binlogs
// @Produce json
// @Param namespace query string false "Kubernetes namespace; defaults to 'default' when omitted"
// @Success 200 {array} XStoreBackupBinlogDTO "List of XStore binlog backups"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backup-binlogs [get]
func ListBackupBinlogs(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", "")

	items, err := backupBinlogSvc(c).List(c.Request.Context(), cli, ns)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, items)
}

// CreateBackupBinlog creates a binlog backup for an XStore.
// @Summary Create XStore binlog backup
// @Description Create a new binlog backup for an XStore.
// @Tags xstores, backup-binlogs
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace; defaults to 'default' when omitted"
// @Param body body XStoreBackupBinlogSpecDTO true "XStore binlog backup specification"
// @Success 201 {object} XStoreBackupBinlogDTO "Created XStore binlog backup"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backup-binlogs [post]
func CreateBackupBinlog(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", "default")

	var obj polardbxv1.XStoreBackupBinlog
	if err := c.ShouldBindJSON(&obj); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	created, err := backupBinlogSvc(c).Create(c.Request.Context(), cli, ns, &obj)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, created)
}

// GetBackupBinlog gets a single XStore binlog backup by namespace and name.
// @Summary Get XStore binlog backup
// @Description Get a binlog backup by namespace and name.
// @Tags xstores, backup-binlogs
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the binlog backup"
// @Param name path string true "Name of the binlog backup"
// @Success 200 {object} XStoreBackupBinlogDTO "XStore binlog backup"
// @Failure 404 {object} apierr.ErrorResponse "Binlog backup not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backup-binlogs/{namespace}/{name} [get]
func GetBackupBinlog(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	item, err := backupBinlogSvc(c).Get(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, item)
}

// UpdateBackupBinlog updates an existing XStore binlog backup.
// @Summary Update XStore binlog backup
// @Description Update an existing XStore binlog backup resource.
// @Tags xstores, backup-binlogs
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the binlog backup"
// @Param name path string true "Name of the binlog backup"
// @Param body body XStoreBackupBinlogSpecDTO true "Updated XStore binlog backup specification"
// @Success 200 {object} XStoreBackupBinlogDTO "Updated XStore binlog backup"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 404 {object} apierr.ErrorResponse "Binlog backup not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backup-binlogs/{namespace}/{name} [put]
func UpdateBackupBinlog(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")

	var obj polardbxv1.XStoreBackupBinlog
	if err := c.ShouldBindJSON(&obj); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	updated, err := backupBinlogSvc(c).Update(c.Request.Context(), cli, ns, &obj)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, updated)
}

// DeleteBackupBinlog deletes an XStore binlog backup.
// @Summary Delete XStore binlog backup
// @Description Delete an XStore binlog backup by namespace and name.
// @Tags xstores, backup-binlogs
// @Param namespace path string true "Kubernetes namespace of the binlog backup"
// @Param name path string true "Name of the binlog backup"
// @Success 200 {object} MessageResponseDTO "Deletion confirmation"
// @Failure 404 {object} apierr.ErrorResponse "Binlog backup not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backup-binlogs/{namespace}/{name} [delete]
func DeleteBackupBinlog(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	if err := backupBinlogSvc(c).Delete(c.Request.Context(), cli, ns, name); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "xstore backup binlog deleted"})
}
