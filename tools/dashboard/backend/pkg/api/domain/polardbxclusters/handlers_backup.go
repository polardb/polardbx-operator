package polardbxclusters

import (
	"net/http"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"

	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/services"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

// ListBackups lists backups for a given cluster by delegating to BackupService.
// @Summary List backups for a cluster
// @Description List all backups that belong to the specified PolarDB-X cluster.
// @Tags polardbxclusters, backups
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the cluster"
// @Param name path string true "Name of the PolarDB-X cluster"
// @Success 200 {array} map[string]any "List of backups"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func ListBackups(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	clusterName := c.Param("name")
	namespace := c.Param("namespace")

	backups, err := svc.ListBackups(c.Request.Context(), cli, namespace, clusterName)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, backups)
}

// CreateBackup creates a new backup for a given cluster.
// @Summary Create backup
// @Description Create a PolarDB-X backup resource bound to the specified cluster.
// @Tags polardbxclusters, backups
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the cluster"
// @Param name path string true "Name of the PolarDB-X cluster"
// @Param body body map[string]any true "Backup specification"
// @Success 201 {object} map[string]any "Created backup"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body or parameters"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func CreateBackup(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	var backup polardbxv1.PolarDBXBackup
	if err := c.ShouldBindJSON(&backup); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	clusterName := c.Param("name")
	namespace := c.Param("namespace")

	created, err := svc.CreateBackup(c.Request.Context(), cli, namespace, clusterName, &backup)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, created)
}

// GetBackupAdvice returns backup-related recommendations for a cluster.
// @Summary Get backup advice
// @Description Get recommendations and advice for backup strategies of a given cluster.
// @Tags polardbxclusters, backups
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the cluster"
// @Param name path string true "Name of the PolarDB-X cluster"
// @Success 200 {object} map[string]any "Advice payload"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func GetBackupAdvice(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	clusterName := c.Param("name")

	hasFollower, role, reason, err := svc.GetBackupAdvice(c.Request.Context(), cli, namespace, clusterName)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	resp := gin.H{"hasFollower": hasFollower, "role": role}
	if reason != nil {
		resp["reason"] = reason
	}
	apierr.OK(c, resp)
}

// ValidateBackup validates a backup specification using a dry-run request.
// @Summary Validate backup
// @Description Validate a backup specification by performing a dry-run create against Kubernetes.
// @Tags polardbxclusters, backups
// @Accept json
// @Produce json
// @Param namespace query string false "Target namespace for validation; falls back to backup namespace or 'default'"
// @Param body body map[string]any true "Backup specification to validate"
// @Success 200 {object} map[string]bool "Validation result (valid: true)"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body or parameters"
// @Failure 422 {object} apierr.ErrorResponse "Backup validation failed"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func ValidateBackup(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	var backup polardbxv1.PolarDBXBackup
	if err := c.ShouldBindJSON(&backup); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	ns := c.Query("namespace")
	if ns == "" {
		ns = backup.Namespace
	}
	if ns == "" {
		ns = "default"
	}

	if err := svc.ValidateBackup(c.Request.Context(), cli, ns, &backup); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"valid": true})
}

// StreamBackupEvents streams backup phase changes and events using server-sent events.
// @Summary Stream backup events
// @Description Stream backup phase changes and events as Server-Sent Events (SSE).
// @Tags polardbxclusters, backups
// @Produce text/event-stream
// @Param namespace path string true "Kubernetes namespace of the backup"
// @Param name path string true "Name of the PolarDB-X backup"
// @Success 200 {string} string "SSE stream of backup events"
// @Failure 404 {object} apierr.ErrorResponse "Backup not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error or streaming unsupported"
func StreamBackupEvents(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	name := c.Param("name")
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		apierr.AbortWithError(c, apierr.InternalServiceError("streaming unsupported", nil))
		return
	}
	// Set standard SSE headers before starting the stream.
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	if err := svc.StreamEvents(c.Request.Context(), cli, namespace, name, c.Writer, flusher.Flush); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
}

// GetBackupMetrics returns coarse-grained backup progress metrics for a backup.
// @Summary Get backup metrics
// @Description Get coarse-grained backup progress and aggregated child XStore backup status.
// @Tags polardbxclusters, backups
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the backup"
// @Param name path string true "Name of the PolarDB-X backup"
// @Success 200 {object} map[string]any "Progress metrics and child backup stats"
// @Failure 404 {object} apierr.ErrorResponse "Backup not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func GetBackupMetrics(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	resp, err := svc.GetBackupMetrics(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, resp)
}

// GetBinlogMetrics returns binlog backup related metrics for a cluster.
// @Summary Get binlog backup metrics
// @Description Get metrics related to binlog backup for the specified cluster.
// @Tags polardbxclusters, backups
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the cluster"
// @Param name path string true "Name of the PolarDB-X cluster"
// @Success 200 {object} map[string]any "Binlog backup metrics"
// @Failure 404 {object} apierr.ErrorResponse "Cluster or metrics not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func GetBinlogMetrics(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.DefaultQuery("namespace", "")
	nowStr := c.DefaultQuery("now", "")
	var now time.Time
	if nowStr != "" {
		if t, err := time.Parse(time.RFC3339, nowStr); err == nil {
			now = t
		}
	}
	resp, err := svc.GetBinlogMetrics(c.Request.Context(), cli, namespace, now)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, resp)
}

// DeleteBackup deletes a PolarDB-X backup resource.
// @Summary Delete backup
// @Description Delete the specified PolarDB-X backup resource.
// @Tags polardbxclusters, backups
// @Param namespace path string true "Kubernetes namespace of the backup"
// @Param name path string true "Name of the PolarDB-X backup"
// @Success 204 "Backup deleted successfully"
// @Failure 404 {object} apierr.ErrorResponse "Backup not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func DeleteBackup(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := svc.DeleteBackup(c.Request.Context(), cli, ns, name); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "backup deleted successfully"})
}

// ForceDeleteBackup forcefully deletes a backup by removing finalizers and then deleting the resource.
// @Summary Force delete backup
// @Description Remove finalizers from the backup and then delete it, bypassing normal protection.
// @Tags polardbxclusters, backups
// @Param namespace path string true "Kubernetes namespace of the backup"
// @Param name path string true "Name of the PolarDB-X backup"
// @Success 202 {object} map[string]any "Deletion requested"
// @Failure 404 {object} apierr.ErrorResponse "Backup not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func ForceDeleteBackup(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := svc.ForceDeleteBackup(c.Request.Context(), cli, ns, name); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "backup finalizers removed and deletion triggered"})
}

// ListSchedules lists backup schedules for a cluster or namespace.
// @Summary List backup schedules
// @Description List backup schedules, optionally filtered by namespace and name.
// @Tags polardbxclusters, backups, schedules
// @Produce json
// @Param namespace query string false "Filter by namespace"
// @Param name query string false "Filter by schedule name"
// @Success 200 {array} map[string]any "List of backup schedules"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func ListSchedules(c *gin.Context) {
	svc := services.NewBackupScheduleService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	items, err := svc.ListSchedules(c.Request.Context(), cli, ns)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, items)
}

// CreateSchedule creates a new backup schedule.
// @Summary Create backup schedule
// @Description Create a new PolarDB-X backup schedule resource.
// @Tags polardbxclusters, backups, schedules
// @Accept json
// @Produce json
// @Param body body map[string]any true "Backup schedule specification"
// @Success 201 {object} map[string]any "Created backup schedule"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body or parameters"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func CreateSchedule(c *gin.Context) {
	svc := services.NewBackupScheduleService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.PolarDBXBackupSchedule
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	created, err := svc.CreateSchedule(c.Request.Context(), cli, ns, &body)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, created)
}

// GetSchedule gets a specific backup schedule.
// @Summary Get backup schedule
// @Description Get a backup schedule by namespace and name.
// @Tags polardbxclusters, backups, schedules
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the schedule"
// @Param name path string true "Name of the backup schedule"
// @Success 200 {object} map[string]any "Backup schedule"
// @Failure 404 {object} apierr.ErrorResponse "Schedule not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func GetSchedule(c *gin.Context) {
	svc := services.NewBackupScheduleService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	item, err := svc.GetSchedule(c.Request.Context(), cli, ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, item)
}

// UpdateSchedule updates an existing backup schedule.
// @Summary Update backup schedule
// @Description Update an existing PolarDB-X backup schedule resource.
// @Tags polardbxclusters, backups, schedules
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the schedule"
// @Param name path string true "Name of the backup schedule"
// @Param body body map[string]any true "Updated schedule specification"
// @Success 200 {object} map[string]any "Updated backup schedule"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body or parameters"
// @Failure 404 {object} apierr.ErrorResponse "Schedule not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func UpdateSchedule(c *gin.Context) {
	svc := services.NewBackupScheduleService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	var body polardbxv1.PolarDBXBackupSchedule
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	updated, err := svc.UpdateSchedule(c.Request.Context(), cli, ns, &body)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, updated)
}

// DeleteSchedule deletes a backup schedule.
// @Summary Delete backup schedule
// @Description Delete a backup schedule by namespace and name.
// @Tags polardbxclusters, backups, schedules
// @Param namespace path string true "Kubernetes namespace of the schedule"
// @Param name path string true "Name of the backup schedule"
// @Success 204 "Backup schedule deleted successfully"
// @Failure 404 {object} apierr.ErrorResponse "Schedule not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func DeleteSchedule(c *gin.Context) {
	svc := services.NewBackupScheduleService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := svc.DeleteSchedule(c.Request.Context(), cli, ns, name); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "backup schedule deleted"})
}

// GetScheduleNextRuns returns the next execution times for backup schedules.
// @Summary Get backup schedule next runs
// @Description Get upcoming execution times for backup schedules, optionally filtered by namespace.
// @Tags polardbxclusters, backups, schedules
// @Produce json
// @Param namespace query string false "Filter by namespace"
// @Success 200 {object} map[string]any "Next-run aggregation"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func GetScheduleNextRuns(c *gin.Context) {
	svc := services.NewBackupScheduleService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.DefaultQuery("namespace", "")
	nowStr := c.DefaultQuery("now", "")
	var now time.Time
	if nowStr != "" {
		if t, err := time.Parse(time.RFC3339, nowStr); err == nil {
			now = t
		}
	}
	items, err := svc.GetNextRuns(c.Request.Context(), cli, namespace, now)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"namespace": namespace, "total": len(items), "schedules": items})
}

// GetBackupOverview returns an aggregated overview of backup status for a cluster.
// @Summary Get backup overview
// @Description Get high-level backup overview information for the specified cluster.
// @Tags polardbxclusters, backups
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the cluster"
// @Param name path string true "Name of the PolarDB-X cluster"
// @Success 200 {object} map[string]any "Backup overview"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func GetBackupOverview(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.DefaultQuery("namespace", "")
	evaluateConnectivity := c.DefaultQuery("evaluateConnectivity", "false") == "true"
	evaluateStorage := c.DefaultQuery("evaluateStorage", "false") == "true"
	systemNS := c.DefaultQuery("systemNamespace", "polardbx-operator-system")
	resp, err := svc.GetBackupOverview(c.Request.Context(), cli, namespace, evaluateConnectivity, evaluateStorage, systemNS)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, resp)
}

// GetClusterBackupState returns a summarized backup state for the given cluster.
// @Summary Get cluster backup state
// @Description Get summarized backup state for the specified cluster, for use in UI.
// @Tags polardbxclusters, backups
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the cluster"
// @Param name path string true "Name of the PolarDB-X cluster"
// @Success 200 {object} map[string]any "Cluster backup state"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func GetClusterBackupState(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.DefaultQuery("namespace", "")
	resp, err := svc.GetClusterBackupState(c.Request.Context(), cli, namespace)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, resp)
}

// ListHpfsSinks lists configured HPFS sinks from the HPFS configuration.
// @Summary List HPFS sinks
// @Description List sinks configured in the HPFS configuration ConfigMap.
// @Tags polardbxclusters, hpfs
// @Produce json
// @Param systemNamespace query string false "Namespace of the HPFS config ConfigMap; defaults to polardbx-operator-system"
// @Success 200 {object} map[string]any "HPFS sink definitions"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error or invalid config"
func ListHpfsSinks(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	systemNS := c.DefaultQuery("systemNamespace", "polardbx-operator-system")
	resp, err := svc.ListHpfsSinks(c.Request.Context(), cli, systemNS)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, resp)
}

// ValidateHpfsSink validates that a given HPFS sink exists in the HPFS configuration.
// @Summary Validate HPFS sink
// @Description Validate that an HPFS sink with given name and type exists in HPFS config.
// @Tags polardbxclusters, hpfs
// @Accept json
// @Produce json
// @Param systemNamespace query string false "Namespace of the HPFS config ConfigMap; defaults to polardbx-operator-system"
// @Param body body map[string]any true "Sink name and type (expects fields: name, type)"
// @Success 200 {object} map[string]any "Validation status and message"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func ValidateHpfsSink(c *gin.Context) {
	svc := services.NewBackupService()
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var req struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	systemNS := c.DefaultQuery("systemNamespace", "polardbx-operator-system")
	resp, err := svc.ValidateHpfsSink(c.Request.Context(), cli, systemNS, req.Name, req.Type)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, resp)
}
