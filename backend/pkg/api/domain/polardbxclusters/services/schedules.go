package services

import (
	"net/http"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	cronv3 "github.com/robfig/cron/v3"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/k8s"
)

// BackupScheduleService 封装 BackupSchedule 相关编排（保持行为不变）
type BackupScheduleService struct{}

func NewBackupScheduleService() *BackupScheduleService { return &BackupScheduleService{} }

func (s *BackupScheduleService) List(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	items, err := k8s.ListPolarDBXBackupSchedulesWithContext(c.Request.Context(), cli, ns)
	if err != nil {
		util.HandleK8sError(c, "failed to list backup schedules", err)
		return
	}
	c.JSON(http.StatusOK, items)
}

func (s *BackupScheduleService) Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "default")
	var body polardbxv1.PolarDBXBackupSchedule
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid schedule", "details": err.Error()})
		return
	}
	body.Namespace = ns
	created, err := k8s.CreatePolarDBXBackupScheduleWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		util.HandleK8sError(c, "failed to create backup schedule", err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

func (s *BackupScheduleService) Get(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	item, err := k8s.GetPolarDBXBackupScheduleWithContext(c.Request.Context(), cli, ns, name)
	if err != nil {
		util.HandleK8sError(c, "failed to get backup schedule", err)
		return
	}
	c.JSON(http.StatusOK, item)
}

func (s *BackupScheduleService) Update(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	var body polardbxv1.PolarDBXBackupSchedule
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid schedule", "details": err.Error()})
		return
	}
	body.Namespace = ns
	updated, err := k8s.UpdatePolarDBXBackupScheduleWithContext(c.Request.Context(), cli, ns, &body)
	if err != nil {
		util.HandleK8sError(c, "failed to update backup schedule", err)
		return
	}
	c.JSON(http.StatusOK, updated)
}

func (s *BackupScheduleService) Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := k8s.DeletePolarDBXBackupScheduleWithContext(c.Request.Context(), cli, ns, name); err != nil {
		util.HandleK8sError(c, "failed to delete backup schedule", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "backup schedule deleted"})
}

// GetNextRuns returns per-schedule next run time (uses status.nextBackupTime; cron-parsed otherwise)
func (s *BackupScheduleService) GetNextRuns(c *gin.Context) {
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
	if now.IsZero() {
		now = time.Now()
	}
	var list polardbxv1.PolarDBXBackupScheduleList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := cli.List(c.Request.Context(), &list, opts...); err != nil {
		util.HandleK8sError(c, "failed to list backup schedules", err)
		return
	}
	items := make([]map[string]any, 0, len(list.Items))
	for _, it := range list.Items {
		entry := map[string]any{
			"name":      it.Name,
			"namespace": it.Namespace,
			"schedule":  it.Spec.Schedule,
		}
		if it.Status.NextBackupTime != nil && !it.Status.NextBackupTime.Time.IsZero() {
			entry["nextRunTime"] = it.Status.NextBackupTime.Time.Format(time.RFC3339)
		} else {
			if it.Spec.Schedule != "" {
				if sch, err := cronv3.ParseStandard(it.Spec.Schedule); err == nil {
					next := sch.Next(now)
					entry["nextRunTime"] = next.UTC().Format(time.RFC3339)
				} else {
					entry["nextRunTime"] = "pending_implementation"
				}
			} else {
				entry["nextRunTime"] = "pending_implementation"
			}
		}
		items = append(items, entry)
	}
	c.JSON(http.StatusOK, gin.H{"namespace": namespace, "total": len(items), "schedules": items})
}
