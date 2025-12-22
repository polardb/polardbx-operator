package xstores

import (
	"polardbx-dashboard-backend/pkg/api/domain/xstores/k8srepo"
	"polardbx-dashboard-backend/pkg/api/domain/xstores/services"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/provider"
	"polardbx-dashboard-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
)

// backupsSvc gets BackupsService from Provider
func backupsSvc(c *gin.Context) *services.BackupsService {
	return provider.Must(c).BackupsService(c)
}

// ListBackups lists XStore backups in the given namespace.
// @Summary List XStore backups
// @Description List backups for XStore instances in the specified or default namespace.
// @Tags xstores, backups
// @Produce json
// @Param namespace query string false "Kubernetes namespace; defaults to 'default' when omitted"
// @Success 200 {array} XStoreBackupDTO "List of XStore backups"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backups [get]
func ListBackups(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")

	items, err := backupsSvc(c).List(c.Request.Context(), cli, ns)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, items)
}

// CreateBackup creates an XStore backup.
// @Summary Create XStore backup
// @Description Create a new backup for an XStore.
// @Tags xstores, backups
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace; defaults to 'default' when omitted"
// @Param body body XStoreBackupSpecDTO true "XStore backup specification"
// @Success 201 {object} XStoreBackupDTO "Created XStore backup"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backups [post]
func CreateBackup(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")

	var body k8srepo.XStoreBackupAlias
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	obj := body.As()

	created, err := backupsSvc(c).Create(c.Request.Context(), cli, ns, obj)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, created)
}

// GetBackup gets a single XStore backup by namespace and name.
// @Summary Get XStore backup
// @Description Get a backup for a given XStore by namespace and name.
// @Tags xstores, backups
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the backup"
// @Param name path string true "Name of the backup"
// @Success 200 {object} XStoreBackupDTO "XStore backup"
// @Failure 404 {object} apierr.ErrorResponse "Backup not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backups/{namespace}/{name} [get]
func GetBackup(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	item, err := backupsSvc(c).Get(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, item)
}

// UpdateBackup updates an existing XStore backup.
// @Summary Update XStore backup
// @Description Update an existing XStore backup resource.
// @Tags xstores, backups
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the backup"
// @Param name path string true "Name of the backup"
// @Param body body XStoreBackupSpecDTO true "Updated XStore backup specification"
// @Success 200 {object} XStoreBackupDTO "Updated XStore backup"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 404 {object} apierr.ErrorResponse "Backup not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backups/{namespace}/{name} [put]
func UpdateBackup(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")

	var body k8srepo.XStoreBackupAlias
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	obj := body.As()
	obj.Namespace = ns

	updated, err := backupsSvc(c).Update(c.Request.Context(), cli, ns, obj)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, updated)
}

// DeleteBackup deletes an XStore backup.
// @Summary Delete XStore backup
// @Description Delete an XStore backup by namespace and name.
// @Tags xstores, backups
// @Param namespace path string true "Kubernetes namespace of the backup"
// @Param name path string true "Name of the backup"
// @Success 200 {object} MessageResponseDTO "Deletion confirmation"
// @Failure 404 {object} apierr.ErrorResponse "Backup not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backups/{namespace}/{name} [delete]
func DeleteBackup(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	if err := backupsSvc(c).Delete(c.Request.Context(), cli, ns, name); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "xstore backup deleted"})
}

// ForceDeleteBackup forcefully deletes an XStore backup by removing protection and then deleting it.
// @Summary Force delete XStore backup
// @Description Force delete an XStore backup resource by bypassing safeguards.
// @Tags xstores, backups
// @Param namespace path string true "Kubernetes namespace of the backup"
// @Param name path string true "Name of the backup"
// @Success 202 {object} MessageResponseDTO "Deletion requested"
// @Failure 404 {object} apierr.ErrorResponse "Backup not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backups/{namespace}/{name}/force-delete [post]
func ForceDeleteBackup(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	if err := backupsSvc(c).ForceDelete(c.Request.Context(), cli, ns, name); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "xstore backup finalizers removed"})
}

// GetBackupRemoteInfo gets remote storage information for an XStore backup.
// @Summary Get XStore backup remote info
// @Description Get remote storage information for an XStore backup (e.g., remote path or location).
// @Tags xstores, backups
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the backup"
// @Param name path string true "Name of the backup"
// @Success 200 {object} BackupRemoteInfoDTO "Remote backup information"
// @Failure 404 {object} apierr.ErrorResponse "Backup not found"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Kubernetes error"
// @Router /api/v1/xstores/backups/{namespace}/{name}/remote-info [get]
func GetBackupRemoteInfo(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	info, err := backupsSvc(c).RemoteInfo(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, info)
}
