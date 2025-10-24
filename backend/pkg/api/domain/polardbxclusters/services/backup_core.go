package services

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/k8s"
)

// BackupService：集群备份核心操作
type BackupService struct{}

func NewBackupService() *BackupService { return &BackupService{} }

// List 列出集群的备份
func (s *BackupService) List(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	clusterName := c.Param("name")
	namespace := c.Param("namespace")
	backups, err := k8s.ListPolarDBXBackupsWithContext(c.Request.Context(), cli, namespace, clusterName)
	if err != nil {
		util.HandleK8sError(c, "failed to list backups", err)
		return
	}
	c.JSON(http.StatusOK, backups)
}

// Create 为集群创建备份
func (s *BackupService) Create(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var backup polardbxv1.PolarDBXBackup
	if err := c.ShouldBindJSON(&backup); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to parse backup data", "details": err.Error()})
		return
	}
	clusterName := c.Param("name")
	namespace := c.Param("namespace")
	backup.Spec.Cluster.Name = clusterName
	created, err := k8s.CreatePolarDBXBackupWithContext(c.Request.Context(), cli, namespace, &backup)
	if err != nil {
		util.HandleK8sError(c, "failed to create backup", err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

// Validate 通过 dry-run 校验备份
func (s *BackupService) Validate(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var backup polardbxv1.PolarDBXBackup
	if err := c.ShouldBindJSON(&backup); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to parse backup data", "details": err.Error()})
		return
	}
	ns := c.Query("namespace")
	if ns == "" {
		ns = backup.Namespace
	}
	if ns == "" {
		ns = "default"
	}
	if _, err := k8s.CreatePolarDBXBackupDryRunWithContext(c.Request.Context(), cli, ns, &backup); err != nil {
		util.HandleK8sError(c, "backup validation failed", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"valid": true})
}

// StreamEvents 以 SSE 方式输出备份事件（轮询）
func (s *BackupService) StreamEvents(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	name := c.Param("name")
	w := c.Writer
	flusher, ok := w.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "streaming unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	write := func(event string, payload gin.H) {
		payload["timestamp"] = time.Now().UTC().Format(time.RFC3339)
		b, _ := json.Marshal(gin.H{"type": event, "payload": payload})
		_, _ = w.Write([]byte("event: " + event + "\n"))
		_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
		flusher.Flush()
	}
	ctx := c.Request.Context()
	var lastPhase string
	var backup polardbxv1.PolarDBXBackup
	if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &backup); err == nil {
		lastPhase = strings.ToLower(string(backup.Status.Phase))
		write("phaseChanged", gin.H{"phase": lastPhase})
	} else {
		write("error", gin.H{"message": "backup not found", "details": err.Error()})
		return
	}
	keep := time.NewTicker(10 * time.Second)
	defer keep.Stop()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-keep.C:
			_, _ = w.Write([]byte(": ping\n\n"))
			flusher.Flush()
		case <-tick.C:
			var cur polardbxv1.PolarDBXBackup
			if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cur); err != nil {
				write("error", gin.H{"message": "failed to get backup", "details": err.Error()})
				return
			}
			phase := strings.ToLower(string(cur.Status.Phase))
			if phase != lastPhase {
				lastPhase = phase
				write("phaseChanged", gin.H{"phase": phase})
			}
			switch phase {
			case "succeeded", "completed", "finished", "failed", "deleting":
				return
			}
		}
	}
}

// GetMetrics 获取粗粒度备份进度
func (s *BackupService) GetMetrics(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	var backup polardbxv1.PolarDBXBackup
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, &backup); err != nil {
		util.HandleK8sError(c, "failed to get backup", err)
		return
	}
	var xsList polardbxv1.XStoreBackupList
	_ = cli.List(c.Request.Context(), &xsList, client.InNamespace(ns))
	total, finished, failed := 0, 0, 0
	for i := range xsList.Items {
		b := xsList.Items[i]
		if b.Labels["polardbx/top-backup"] != name {
			continue
		}
		total++
		p := strings.ToLower(string(b.Status.Phase))
		if p == "finished" || p == "completed" {
			finished++
		} else if p == "failed" {
			failed++
		}
	}
	phase := strings.ToLower(string(backup.Status.Phase))
	mapping := map[string]int{"": 5, "new": 5, "fullbackuping": 25, "collecting": 50, "calculating": 65, "binlogbackuping": 85, "metadatabackuping": 95, "finished": 100, "succeeded": 100, "completed": 100, "failed": 0, "deleting": 0}
	progress := mapping[phase]
	if progress > 0 && progress < 100 && total > 0 {
		fromChildren := int(float64(finished) / float64(total) * 100.0)
		progress = (progress + fromChildren) / 2
		if failed > 0 && phase != "finished" && progress > 90 {
			progress = 90
		}
	}
	c.JSON(http.StatusOK, gin.H{"phase": phase, "progress": progress, "estimated": true, "children": gin.H{"total": total, "finished": finished, "failed": failed}})
}

// ForceDelete 移除 PolarDBXBackup 的 finalizers 并触发删除
func (s *BackupService) ForceDelete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")

	var bk polardbxv1.PolarDBXBackup
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, &bk); err != nil {
		util.HandleK8sError(c, "failed to get backup", err)
		return
	}
	// 清空 finalizers
	bk.SetFinalizers([]string{})
	if err := cli.Update(c.Request.Context(), &bk); err != nil {
		util.HandleK8sError(c, "failed to remove finalizers", err)
		return
	}
	// 触发删除
	if err := k8s.DeletePolarDBXBackupWithContext(c.Request.Context(), cli, ns, name); err != nil {
		util.HandleK8sError(c, "failed to delete backup", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "backup finalizers removed and deletion triggered"})
}

// GetOverview aggregates last 24h KPIs (migrated from legacy)
func (s *BackupService) GetOverview(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.DefaultQuery("namespace", "")
	now := time.Now()
	windowStart := now.Add(-24 * time.Hour)

	var backupList polardbxv1.PolarDBXBackupList
	listOpts := []client.ListOption{}
	if namespace != "" {
		listOpts = append(listOpts, client.InNamespace(namespace))
	}
	if err := cli.List(c.Request.Context(), &backupList, listOpts...); err != nil {
		util.HandleK8sError(c, "failed to list backups", err)
		return
	}

	total24h, success24h, failed24h, runningNow := 0, 0, 0, 0
	prefixes := make([]string, 0, len(backupList.Items))
	for _, b := range backupList.Items {
		switch b.Status.Phase {
		case polardbxv1.FullBackuping, polardbxv1.BackupCollecting, polardbxv1.BackupCalculating, polardbxv1.BinlogBackuping, polardbxv1.MetadataBackuping:
			runningNow++
		}
		if b.Status.StartTime != nil && b.Status.StartTime.Time.After(windowStart) {
			total24h++
			switch b.Status.Phase {
			case polardbxv1.BackupFinished:
				success24h++
			case polardbxv1.BackupFailed:
				failed24h++
			}
			if b.Status.BackupRootPath != "" {
				prefixes = append(prefixes, b.Status.BackupRootPath)
			}
		}
	}
	successRate := 0
	if total24h > 0 {
		successRate = int(float64(success24h)*100.0/float64(total24h) + 0.5)
	}

	// Connectivity/storage evaluation
	kpi := gin.H{"successRate24h": successRate, "running": runningNow, "failed24h": failed24h, "totalBackups24h": total24h, "totalStorage": "pending_implementation", "storageConnectivity": "pending_implementation"}
	if c.DefaultQuery("evaluateConnectivity", "false") == "true" {
		status, detail := s.evaluateStorageConnectivity(c)
		kpi["storageConnectivityStatus"] = status
		kpi["storageConnectivity"] = detail
	}
	if c.DefaultQuery("evaluateStorage", "false") == "true" {
		// For now, we skip online S3 scan here; retain legacy behavior via placeholders
	}
	c.JSON(http.StatusOK, gin.H{"namespace": namespace, "timeWindowHours": 24, "generatedAt": now.Format(time.RFC3339), "kpi": kpi})
}

// GetClusterState aggregates per-cluster backup state: latest full backup, next schedule time, and RPO
func (s *BackupService) GetClusterState(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	namespace := c.DefaultQuery("namespace", "")
	now := time.Now()

	var clusterList polardbxv1.PolarDBXClusterList
	clusterOpts := []client.ListOption{}
	if namespace != "" {
		clusterOpts = append(clusterOpts, client.InNamespace(namespace))
	}
	if err := cli.List(c.Request.Context(), &clusterList, clusterOpts...); err != nil {
		util.HandleK8sError(c, "failed to list clusters", err)
		return
	}

	var backupList polardbxv1.PolarDBXBackupList
	backupOpts := []client.ListOption{}
	if namespace != "" {
		backupOpts = append(backupOpts, client.InNamespace(namespace))
	}
	if err := cli.List(c.Request.Context(), &backupList, backupOpts...); err != nil {
		util.HandleK8sError(c, "failed to list backups", err)
		return
	}

	var scheduleList polardbxv1.PolarDBXBackupScheduleList
	scheduleOpts := []client.ListOption{}
	if namespace != "" {
		scheduleOpts = append(scheduleOpts, client.InNamespace(namespace))
	}
	if err := cli.List(c.Request.Context(), &scheduleList, scheduleOpts...); err != nil {
		util.HandleK8sError(c, "failed to list backup schedules", err)
		return
	}

	latestByCluster := map[string]polardbxv1.PolarDBXBackup{}
	maxLrtByCluster := map[string]time.Time{}
	for _, b := range backupList.Items {
		clusterName := b.Spec.Cluster.Name
		if clusterName == "" {
			continue
		}
		if prev, ok := latestByCluster[clusterName]; ok {
			if isBackupNewer(b, prev) {
				latestByCluster[clusterName] = b
			}
		} else {
			latestByCluster[clusterName] = b
		}
		if b.Status.LatestRecoverableTimestamp != nil {
			lrt := b.Status.LatestRecoverableTimestamp.Time
			if prev, ok := maxLrtByCluster[clusterName]; !ok || lrt.After(prev) {
				maxLrtByCluster[clusterName] = lrt
			}
		}
	}

	nextByCluster := map[string]*metav1.Time{}
	for _, s := range scheduleList.Items {
		name := s.Spec.BackupSpec.Cluster.Name
		if name == "" {
			continue
		}
		if _, exists := nextByCluster[name]; exists {
			continue
		}
		if s.Status.NextBackupTime != nil {
			nextByCluster[name] = s.Status.NextBackupTime
		}
	}

	out := make([]map[string]any, 0, len(clusterList.Items))
	for _, cl := range clusterList.Items {
		entry := map[string]any{
			"clusterName": cl.Name,
			"namespace":   cl.Namespace,
		}
		if lb, ok := latestByCluster[cl.Name]; ok {
			lbInfo := map[string]any{"name": lb.Name, "phase": string(lb.Status.Phase)}
			if lb.Status.StartTime != nil {
				lbInfo["startTime"] = lb.Status.StartTime.Time.Format(time.RFC3339)
			}
			if lb.Status.EndTime != nil {
				lbInfo["endTime"] = lb.Status.EndTime.Time.Format(time.RFC3339)
			}
			if lb.Status.LatestRecoverableTimestamp != nil {
				lbInfo["latestRecoverableTimestamp"] = lb.Status.LatestRecoverableTimestamp.Time.Format(time.RFC3339)
			}
			entry["latestBackup"] = lbInfo
		}
		if nt, ok := nextByCluster[cl.Name]; ok && nt != nil {
			entry["nextScheduledTime"] = nt.Time.Format(time.RFC3339)
		} else {
			entry["nextScheduledTime"] = nil
		}
		if lrt, ok := maxLrtByCluster[cl.Name]; ok && !lrt.IsZero() {
			entry["rpoSeconds"] = int(now.Sub(lrt).Seconds())
		} else {
			entry["rpoSeconds"] = nil
		}
		out = append(out, entry)
	}

	c.JSON(http.StatusOK, gin.H{"namespace": namespace, "total": len(out), "clusters": out})
}

// GetBinlogMetrics aggregates binlog CRs and latest backup LRT per cluster
func (s *BackupService) GetBinlogMetrics(c *gin.Context) {
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

	var binlogList polardbxv1.PolarDBXBackupBinlogList
	binlogOpts := []client.ListOption{}
	if namespace != "" {
		binlogOpts = append(binlogOpts, client.InNamespace(namespace))
	}
	_ = cli.List(c.Request.Context(), &binlogList, binlogOpts...)

	var backupList polardbxv1.PolarDBXBackupList
	backupOpts := []client.ListOption{}
	if namespace != "" {
		backupOpts = append(backupOpts, client.InNamespace(namespace))
	}
	_ = cli.List(c.Request.Context(), &backupList, backupOpts...)

	maxLrtByCluster := map[string]time.Time{}
	for _, b := range backupList.Items {
		clusterName := b.Spec.Cluster.Name
		if clusterName == "" || b.Status.LatestRecoverableTimestamp == nil {
			continue
		}
		lrt := b.Status.LatestRecoverableTimestamp.Time
		if prev, ok := maxLrtByCluster[clusterName]; !ok || lrt.After(prev) {
			maxLrtByCluster[clusterName] = lrt
		}
	}

	items := make([]map[string]any, 0, len(binlogList.Items))
	for _, b := range binlogList.Items {
		clusterName := b.Spec.PxcName
		entry := map[string]any{
			"name":                b.Name,
			"namespace":           b.Namespace,
			"cluster":             clusterName,
			"phase":               string(b.Status.Phase),
			"lastCheckExpireTime": b.Status.CheckExpireFileLastTime,
			"recentDeletedFiles":  b.Status.LastDeletedFiles,
			"recentFiles":         b.Status.LastDeletedFiles,
			"throughputMBps":      "pending_implementation",
		}
		if lrt, ok := maxLrtByCluster[clusterName]; ok && !lrt.IsZero() {
			entry["latestBackupTime"] = lrt.UTC().Format(time.RFC3339)
			entry["lagSeconds"] = int(now.Sub(lrt).Seconds())
		}
		items = append(items, entry)
	}
	c.JSON(http.StatusOK, gin.H{"namespace": namespace, "total": len(items), "binlogs": items})
}

func isBackupNewer(a, b polardbxv1.PolarDBXBackup) bool {
	endA := time.Time{}
	if a.Status.EndTime != nil {
		endA = a.Status.EndTime.Time
	}
	endB := time.Time{}
	if b.Status.EndTime != nil {
		endB = b.Status.EndTime.Time
	}
	if !endA.Equal(endB) {
		return endA.After(endB)
	}
	startA := time.Time{}
	if a.Status.StartTime != nil {
		startA = a.Status.StartTime.Time
	}
	startB := time.Time{}
	if b.Status.StartTime != nil {
		startB = b.Status.StartTime.Time
	}
	if !startA.Equal(startB) {
		return startA.After(startB)
	}
	return a.CreationTimestamp.After(b.CreationTimestamp.Time)
}

// Delete 删除备份
func (s *BackupService) Delete(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	if err := k8s.DeletePolarDBXBackupWithContext(c.Request.Context(), cli, ns, name); err != nil {
		util.HandleK8sError(c, "failed to delete backup", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "backup deleted successfully"})
}
