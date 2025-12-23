package polardbxclusters

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/services"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

// DownloadBackup returns backup download information (recommended commands and API URL).
// @Summary Download backup
// @Description Get download info for a PolarDB-X backup (API URL + suggested CLI command based on sink type).
// @Tags polardbxclusters, backups
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the backup"
// @Param name path string true "Name of the PolarDB-X backup"
// @Success 200 {object} map[string]any "Download information including url, filename and suggested command"
// @Failure 404 {object} apierr.ErrorResponse "Backup or sink not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/backups/{namespace}/{name}/download [get]
func DownloadBackup(c *gin.Context) {
	ns := c.Param("namespace")
	name := c.Param("name")

	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	svc := services.NewBackupService()
	info, err := svc.GetBackupDownloadInfo(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, info)
}

// GetBackupFile streams the backup as a tar.gz file through the API.
// @Summary Get backup file
// @Description Stream a completed backup as tar.gz via the dashboard API (server-side reads from sink).
// @Tags polardbxclusters, backups
// @Produce application/gzip
// @Param namespace path string true "Kubernetes namespace of the backup"
// @Param name path string true "Name of the PolarDB-X backup"
// @Success 200 {file} binary "Backup tar.gz stream"
// @Failure 404 {object} apierr.ErrorResponse "Backup or sink not found"
// @Failure 409 {object} apierr.ErrorResponse "Backup not ready yet"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/backups/{namespace}/{name}/file [get]
func GetBackupFile(c *gin.Context) {
	ns := c.Param("namespace")
	name := c.Param("name")

	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	svc := services.NewBackupService()
	info, err := svc.GetBackupDownloadInfo(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	// Set headers for attachment download.
	c.Header("Content-Type", "application/gzip")
	c.Header("Content-Disposition", `attachment; filename="`+info.Filename+`"`)
	c.Header("Cache-Control", "no-store")
	c.Status(http.StatusOK)

	if err := svc.StreamBackupAsTarGz(c.Request.Context(), cli, ns, name, c.Writer); err != nil {
		// If streaming hasn't started, we can still send a JSON error. Otherwise client will see a broken stream.
		if !c.Writer.Written() {
			apierr.AbortWithError(c, err)
		}
		return
	}
}
