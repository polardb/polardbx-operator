package handler

import (
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"

	"polardbx-dashboard-backend/pkg/api/domain/platform/restore/repository"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

const restoreTimeLayout = "2006-01-02T15:04:05Z"

func normalizeRestoreTime(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", nil
	}

	// Fast-path: already in expected format.
	if _, err := time.Parse(restoreTimeLayout, s); err == nil {
		return s, nil
	}

	// Common UI case: RFC3339 with fractional seconds, e.g. 2024-01-01T12:00:00.123Z
	if strings.HasSuffix(s, "Z") && strings.Contains(s, ".") {
		if dot := strings.LastIndex(s, "."); dot > strings.LastIndex(s, "T") && dot < len(s)-1 {
			trimmed := s[:dot] + "Z"
			if _, err := time.Parse(restoreTimeLayout, trimmed); err == nil {
				return trimmed, nil
			}
		}
	}

	// Fallback: accept RFC3339/RFC3339Nano and normalize to yyyy-MM-ddTHH:mm:ssZ.
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC().Format(restoreTimeLayout), nil
	}

	return "", fmt.Errorf("invalid restore time %q (expected %s)", raw, restoreTimeLayout)
}

// RestoreHandler handles HTTP requests related to restore operations
type RestoreHandler struct {
	repo repository.RestoreRepository
}

// NewRestoreHandler creates a new RestoreHandler
func NewRestoreHandler(repo repository.RestoreRepository) *RestoreHandler {
	return &RestoreHandler{repo: repo}
}

// NewRestoreHandlerFromContext creates handler from gin.Context
func NewRestoreHandlerFromContext(c *gin.Context) (*RestoreHandler, bool) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return nil, false
	}
	repo := repository.NewK8sRestoreRepository(cli)
	return NewRestoreHandler(repo), true
}

// RestoreCluster initiates a full restore from a completed backup.
// @Summary Restore cluster from backup
// @Description Create a new PolarDB-X cluster from a completed backup of the source cluster.
// @Tags restore
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the source cluster"
// @Param name path string true "Name of the source PolarDB-X cluster"
// @Param body body map[string]any true "Restore request payload (expects fields: backupSet/backupName, targetCluster/targetName, storageProvider, time, timezone)"
// @Success 201 {object} map[string]any "Restore initiated successfully"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body or backup not ready"
// @Failure 404 {object} apierr.ErrorResponse "Backup or source cluster not found"
// @Failure 409 {object} apierr.ErrorResponse "Target cluster already exists"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/restore [post]
func RestoreCluster(c *gin.Context) {
	h, ok := NewRestoreHandlerFromContext(c)
	if !ok {
		return
	}
	h.restoreCluster(c)
}

func (h *RestoreHandler) restoreCluster(c *gin.Context) {
	namespace := c.Param("namespace")
	clusterName := c.Param("name")

	var req struct {
		BackupSet       string `json:"backupSet"`
		BackupName      string `json:"backupName"`
		TargetCluster   string `json:"targetCluster,omitempty"`
		TargetName      string `json:"targetName,omitempty"`
		StorageProvider *struct {
			Type   string            `json:"type"`
			Config map[string]string `json:"config"`
		} `json:"storageProvider,omitempty"`
		Time     string `json:"time,omitempty"`
		TimeZone string `json:"timezone,omitempty"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if req.Time != "" {
		normalized, err := normalizeRestoreTime(req.Time)
		if err != nil {
			apierr.AbortWithError(c, apierr.ValidationError(err.Error(), nil))
			return
		}
		req.Time = normalized
	}
	if req.BackupSet == "" && req.BackupName != "" {
		req.BackupSet = req.BackupName
	}
	if req.TargetCluster == "" && req.TargetName != "" {
		req.TargetCluster = req.TargetName
	}
	if strings.TrimSpace(req.BackupSet) == "" {
		apierr.AbortWithError(c, apierr.ValidationError("backupSet (or backupName) is required", nil))
		return
	}

	// Validate that backup exists and is completed
	if req.BackupSet != "" {
		backup, err := h.repo.GetBackup(c.Request.Context(), namespace, req.BackupSet)
		if err != nil {
			// Surface K8s error via unified error conversion path.
			apierr.AbortWithError(c, err)
			return
		}
		if backup.Status.Phase != polardbxv1.BackupFinished {
			apierr.AbortWithError(c, apierr.ValidationError(fmt.Sprintf("backup not ready: phase=%s", backup.Status.Phase), nil))
			return
		}
	}

	target := req.TargetCluster
	if target == "" {
		target = clusterName + "-restored"
	}

	// Ensure target cluster does not exist
	if _, err := h.repo.GetCluster(c.Request.Context(), namespace, target); err == nil {
		apierr.Abort(c, apierr.Conflict(fmt.Sprintf("target cluster %s/%s already exists", namespace, target)))
		return
	}

	// Load source cluster
	source, err := h.repo.GetCluster(c.Request.Context(), namespace, clusterName)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}

	// Build restored cluster
	restored := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      target,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":     "polardbx",
				"app.kubernetes.io/instance": target,
				"polardbx/restore-source":    clusterName,
				"polardbx/restore-backup":    req.BackupSet,
			},
			Annotations: map[string]string{
				"polardbx/restore-from":   fmt.Sprintf("%s/%s", namespace, clusterName),
				"polardbx/restore-backup": req.BackupSet,
				"polardbx/restore-time":   time.Now().Format(time.RFC3339),
			},
		},
		Spec: source.Spec,
	}
	restored.Spec.ServiceName = target
	if restored.Spec.Restore == nil {
		restored.Spec.Restore = &polardbx.RestoreSpec{}
	}
	restored.Spec.Restore.BackupSet = req.BackupSet
	if req.Time != "" {
		restored.Spec.Restore.Time = req.Time
	}
	if req.TimeZone != "" {
		restored.Spec.Restore.TimeZone = req.TimeZone
	}
	restored.Spec.Restore.From = polardbx.PolarDBXRestoreFrom{PolarBDXName: clusterName}

	if err := h.repo.CreateCluster(c.Request.Context(), restored); err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.Created(c, gin.H{"message": "cluster restore initiated successfully", "sourceCluster": clusterName, "targetCluster": target, "namespace": namespace, "backupSet": req.BackupSet, "restoreTime": req.Time, "status": "creating"})
}

// InitiatePITR initiates a point-in-time recovery (PITR) restore.
// @Summary Initiate PITR
// @Description Create a new PolarDB-X cluster restored to a specific point in time.
// @Tags restore
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the source cluster"
// @Param name path string true "Name of the source PolarDB-X cluster"
// @Param body body map[string]any true "PITR request payload (expects fields: time/targetTime, timezone, backupSet/backupName, targetCluster/targetName, storageProvider)"
// @Success 201 {object} map[string]any "PITR initiated successfully"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body, missing time, or backup not ready"
// @Failure 404 {object} apierr.ErrorResponse "Backup or source cluster not found"
// @Failure 409 {object} apierr.ErrorResponse "Target cluster already exists"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/pitr [post]
func InitiatePITR(c *gin.Context) {
	h, ok := NewRestoreHandlerFromContext(c)
	if !ok {
		return
	}
	h.initiatePITR(c)
}

func (h *RestoreHandler) initiatePITR(c *gin.Context) {
	namespace := c.Param("namespace")
	sourceName := c.Param("name")

	var req struct {
		Time            string `json:"time"`
		TargetTime      string `json:"targetTime"`
		TimeZone        string `json:"timezone"`
		BackupSet       string `json:"backupSet"`
		BackupName      string `json:"backupName"`
		TargetCluster   string `json:"targetCluster"`
		TargetName      string `json:"targetName"`
		StorageProvider *struct {
			Type   string            `json:"type"`
			Config map[string]string `json:"config"`
		} `json:"storageProvider"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	// Normalize fields
	if strings.TrimSpace(req.Time) == "" {
		req.Time = req.TargetTime
	}
	normalized, err := normalizeRestoreTime(req.Time)
	if err != nil {
		apierr.AbortWithError(c, apierr.ValidationError(err.Error(), nil))
		return
	}
	req.Time = normalized
	if req.BackupSet == "" && req.BackupName != "" {
		req.BackupSet = req.BackupName
	}
	if req.TargetCluster == "" && req.TargetName != "" {
		req.TargetCluster = req.TargetName
	}
	if strings.TrimSpace(req.Time) == "" {
		apierr.AbortWithError(c, apierr.ValidationError("time (or targetTime) is required", nil))
		return
	}

	// Validate backup
	if strings.TrimSpace(req.BackupSet) != "" {
		backup, err := h.repo.GetBackup(c.Request.Context(), namespace, req.BackupSet)
		if err != nil {
			// Surface K8s error via unified error conversion path.
			apierr.AbortWithError(c, err)
			return
		}
		if backup.Status.Phase != polardbxv1.BackupFinished {
			apierr.AbortWithError(c, apierr.ValidationError(fmt.Sprintf("backup not ready: phase=%s", backup.Status.Phase), nil))
			return
		}
	}

	target := req.TargetCluster
	if target == "" {
		target = sourceName + "-pitr"
	}

	// Ensure target cluster does not exist
	if _, err := h.repo.GetCluster(c.Request.Context(), namespace, target); err == nil {
		apierr.Abort(c, apierr.Conflict(fmt.Sprintf("target cluster exists: %s/%s", namespace, target)))
		return
	}

	// Load source cluster
	source, err := h.repo.GetCluster(c.Request.Context(), namespace, sourceName)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}

	// Build PITR restored cluster
	restored := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      target,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":     "polardbx",
				"app.kubernetes.io/instance": target,
				"polardbx/restore-source":    sourceName,
			},
			Annotations: map[string]string{
				"polardbx/restore-from": fmt.Sprintf("%s/%s", namespace, sourceName),
				"polardbx/restore-time": req.Time,
			},
		},
		Spec: source.Spec,
	}
	restored.Spec.ServiceName = target
	if restored.Spec.Restore == nil {
		restored.Spec.Restore = &polardbx.RestoreSpec{}
	}
	restored.Spec.Restore.Time = req.Time
	if req.TimeZone != "" {
		restored.Spec.Restore.TimeZone = req.TimeZone
	}
	if req.BackupSet != "" {
		restored.Spec.Restore.BackupSet = req.BackupSet
		if restored.Labels == nil {
			restored.Labels = map[string]string{}
		}
		restored.Labels["polardbx/restore-backup"] = req.BackupSet
	}
	restored.Spec.Restore.From = polardbx.PolarDBXRestoreFrom{PolarBDXName: sourceName}

	if err := h.repo.CreateCluster(c.Request.Context(), restored); err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}

	apierr.Created(c, gin.H{
		"message":       "PITR initiated successfully",
		"sourceCluster": sourceName,
		"targetCluster": target,
		"namespace":     namespace,
		"pitrTime":      req.Time,
		"restoreSpec": gin.H{
			"time":      req.Time,
			"timezone":  req.TimeZone,
			"backupSet": req.BackupSet,
		},
	})
}

// GetRestoreStatus returns high-level restore status for a cluster.
// @Summary Get restore status
// @Description Get restore-related status and phase for a (potentially restored) cluster.
// @Tags restore
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the cluster"
// @Param name path string true "Name of the cluster"
// @Success 200 {object} map[string]any "Restore status payload"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/restore-status [get]
func GetRestoreStatus(c *gin.Context) {
	h, ok := NewRestoreHandlerFromContext(c)
	if !ok {
		return
	}
	h.getRestoreStatus(c)
}

func (h *RestoreHandler) getRestoreStatus(c *gin.Context) {
	ns := c.Param("namespace")
	name := c.Param("name")

	cluster, err := h.repo.GetCluster(c.Request.Context(), ns, name)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}

	isRestore := false
	var restoreSpec *polardbx.RestoreSpec
	var sourceCluster, restoreBackup, restoreTime string

	if cluster.Spec.Restore != nil {
		isRestore = true
		restoreSpec = cluster.Spec.Restore
	}
	if cluster.Labels != nil {
		if s, ok := cluster.Labels["polardbx/restore-source"]; ok {
			sourceCluster = s
			isRestore = true
		}
		if s, ok := cluster.Labels["polardbx/restore-backup"]; ok {
			restoreBackup = s
		}
		if s, ok := cluster.Labels["polardbx/restore-time"]; ok {
			restoreTime = s
		}
	}
	if cluster.Annotations != nil {
		if s, ok := cluster.Annotations["polardbx/restore-backup"]; ok && restoreBackup == "" {
			restoreBackup = s
		}
	}
	if restoreSpec != nil {
		if restoreSpec.BackupSet != "" {
			restoreBackup = restoreSpec.BackupSet
		}
		if restoreSpec.Time != "" {
			restoreTime = restoreSpec.Time
		}
		if restoreSpec.From.PolarBDXName != "" {
			sourceCluster = restoreSpec.From.PolarBDXName
		}
	}

	phase := "Unknown"
	stage := "Unknown"
	isRestoring := false
	if cluster.Status.Phase != "" {
		switch cluster.Status.Phase {
		case polardbx.PhasePending:
			phase = "Pending"
			isRestoring = true
		case polardbx.PhaseCreating:
			phase = "Creating"
			stage = "Initializing"
			isRestoring = true
		case polardbx.PhaseRunning:
			if isRestore {
				phase = "Completed"
				stage = "Running"
			} else {
				phase = "Running"
				stage = "Normal"
			}
		case polardbx.PhaseFailed:
			phase = "Failed"
			stage = "Error"
		default:
			phase = string(cluster.Status.Phase)
		}
		if strings.EqualFold(phase, "restoring") {
			stage = "Creating"
			isRestoring = true
		}
	}

	conditions := make([]map[string]any, 0)
	for _, cond := range cluster.Status.Conditions {
		conditions = append(conditions, map[string]any{
			"type":               cond.Type,
			"status":             cond.Status,
			"lastTransitionTime": cond.LastTransitionTime.Format(time.RFC3339),
			"reason":             cond.Reason,
			"message":            cond.Message,
		})
	}

	resp := gin.H{
		"clusterName":        name,
		"namespace":          ns,
		"phase":              phase,
		"stage":              stage,
		"isRestoring":        isRestoring,
		"observedGeneration": cluster.Status.ObservedGeneration,
		"conditions":         conditions,
	}

	if isRestore {
		specInfo := gin.H{}
		if restoreBackup != "" {
			specInfo["backupSet"] = restoreBackup
		}
		if restoreTime != "" {
			specInfo["time"] = restoreTime
		}
		if sourceCluster != "" {
			specInfo["from"] = gin.H{"polardbxName": sourceCluster}
		}
		if restoreSpec != nil && restoreSpec.TimeZone != "" {
			specInfo["timezone"] = restoreSpec.TimeZone
		}
		resp["restoreSpec"] = specInfo
		resp["isRestoreCluster"] = true
		resp["sourceCluster"] = sourceCluster
	} else {
		resp["isRestoreCluster"] = false
	}
	apierr.OK(c, resp)
}

// ListJobs lists restore-related clusters as logical restore jobs.
// @Summary List restore jobs
// @Description List clusters that are or were created via restore flows, as logical restore jobs.
// @Tags restore
// @Produce json
// @Param namespace query string false "Kubernetes namespace filter; list all namespaces if omitted"
// @Success 200 {object} map[string]any "List of restore jobs"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/restore-jobs [get]
func ListJobs(c *gin.Context) {
	h, ok := NewRestoreHandlerFromContext(c)
	if !ok {
		return
	}
	h.listJobs(c)
}

func (h *RestoreHandler) listJobs(c *gin.Context) {
	ns := c.DefaultQuery("namespace", "")

	clusters, err := h.repo.ListClusters(c.Request.Context(), ns)
	if err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}

	jobs := make([]gin.H, 0)
	for _, cl := range clusters {
		isRestore := cl.Spec.Restore != nil || (cl.Labels != nil && (cl.Labels["polardbx/restore-source"] != ""))
		if !isRestore {
			continue
		}
		job := gin.H{"name": cl.Name, "namespace": cl.Namespace, "phase": string(cl.Status.Phase)}
		jobs = append(jobs, job)
	}
	apierr.OK(c, gin.H{"items": jobs, "count": len(jobs)})
}

// GetJob returns restore status for a single restore job (cluster).
// @Summary Get restore job
// @Description Get restore status for a single restore job identified by namespace and name.
// @Tags restore
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the restore job"
// @Param name path string true "Name of the restore job (cluster name)"
// @Success 200 {object} map[string]any "Restore job status"
// @Failure 404 {object} apierr.ErrorResponse "Restore job not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/restore-jobs/{namespace}/{name} [get]
func GetJob(c *gin.Context) {
	GetRestoreStatus(c)
}

// CancelJob marks a restore job as cancel requested.
// @Summary Cancel restore job
// @Description Request cancellation of a restore job (best-effort, depends on implementation).
// @Tags restore
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the restore job"
// @Param name path string true "Name of the restore job"
// @Success 200 {object} map[string]any "Cancel requested"
// @Failure 404 {object} apierr.ErrorResponse "Restore job not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/restore-jobs/{namespace}/{name} [delete]
func CancelJob(c *gin.Context) {
	h, ok := NewRestoreHandlerFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	// Best-effort cancellation: delete the restore cluster CR (the "job").
	// The operator will handle cleanup via its normal deletion flow.
	cluster, err := h.repo.GetCluster(c.Request.Context(), ns, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	isRestore := cluster.Spec.Restore != nil || (cluster.Labels != nil && (cluster.Labels["polardbx/restore-source"] != ""))
	if !isRestore {
		apierr.AbortWithError(c, apierr.ValidationError("cluster is not a restore job", gin.H{"namespace": ns, "name": name}))
		return
	}
	if err := h.repo.DeleteCluster(c.Request.Context(), ns, name); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	apierr.OK(c, gin.H{
		"message":   "cancel requested",
		"namespace": ns,
		"name":      name,
		"status":    "deleted",
	})
}
