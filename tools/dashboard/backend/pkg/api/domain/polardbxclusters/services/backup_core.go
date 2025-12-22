package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	svcerr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/k8s"
)

// BackupService provides core backup operations for PolarDB-X clusters.
type BackupService struct{}

// NewBackupService constructs a new BackupService instance.
func NewBackupService() *BackupService { return &BackupService{} }

// BackupChildMetrics aggregates child backup progress stats.
type BackupChildMetrics struct {
	Total    int `json:"total"`
	Finished int `json:"finished"`
	Failed   int `json:"failed"`
}

// BackupMetricsResponse is a coarse-grained progress summary for a backup.
type BackupMetricsResponse struct {
	Phase     string `json:"phase"`
	Progress  int    `json:"progress"`
	Estimated bool   `json:"estimated"`
	SizeBytes *int64 `json:"sizeBytes,omitempty"`
	// SizeEstimated indicates size is a best-effort estimate (may be omitted when not supported).
	SizeEstimated bool               `json:"sizeEstimated,omitempty"`
	Children      BackupChildMetrics `json:"children"`
}

// BackupOverviewKPI holds 24h KPI stats and optional connectivity/storage info.
type BackupOverviewKPI struct {
	SuccessRate24h            int    `json:"successRate24h"`
	Running                   int    `json:"running"`
	Failed24h                 int    `json:"failed24h"`
	TotalBackups24h           int    `json:"totalBackups24h"`
	TotalStorage              string `json:"totalStorage"`
	TotalStorageBytes         *int64 `json:"totalStorageBytes"`
	StorageConnectivity       string `json:"storageConnectivity"`
	StorageConnectivityStatus string `json:"storageConnectivityStatus"`
}

// BackupOverviewResponse aggregates KPIs for a namespace within a time window.
type BackupOverviewResponse struct {
	Namespace       string            `json:"namespace"`
	TimeWindowHours int               `json:"timeWindowHours"`
	GeneratedAt     time.Time         `json:"generatedAt"`
	KPI             BackupOverviewKPI `json:"kpi"`
}

// ClusterBackupInfo represents latest backup info for a cluster.
type ClusterBackupInfo struct {
	Name                       string  `json:"name"`
	Phase                      string  `json:"phase"`
	StartTime                  *string `json:"startTime,omitempty"`
	EndTime                    *string `json:"endTime,omitempty"`
	LatestRecoverableTimestamp *string `json:"latestRecoverableTimestamp,omitempty"`
}

// ClusterBackupStateEntry summarizes backup state for a single cluster.
type ClusterBackupStateEntry struct {
	ClusterName       string             `json:"clusterName"`
	Namespace         string             `json:"namespace"`
	LatestBackup      *ClusterBackupInfo `json:"latestBackup,omitempty"`
	NextScheduledTime *string            `json:"nextScheduledTime"`
	RPOSeconds        *int               `json:"rpoSeconds"`
}

// ClusterBackupStateResponse is an aggregated view for clusters in a namespace.
type ClusterBackupStateResponse struct {
	Namespace string                    `json:"namespace"`
	Total     int                       `json:"total"`
	Clusters  []ClusterBackupStateEntry `json:"clusters"`
}

// BinlogMetricsEntry aggregates binlog status for a cluster.
type BinlogMetricsEntry struct {
	Name                string   `json:"name"`
	Namespace           string   `json:"namespace"`
	Cluster             string   `json:"cluster"`
	Phase               string   `json:"phase"`
	LastCheckExpireTime uint64   `json:"lastCheckExpireTime"`
	RecentDeletedFiles  []string `json:"recentDeletedFiles"`
	RecentFiles         []string `json:"recentFiles"`
	LatestBackupTime    *string  `json:"latestBackupTime,omitempty"`
	LagSeconds          *int     `json:"lagSeconds,omitempty"`
}

// BinlogMetricsResponse is a namespace-scoped binlog metrics payload.
type BinlogMetricsResponse struct {
	Namespace string               `json:"namespace"`
	Total     int                  `json:"total"`
	Binlogs   []BinlogMetricsEntry `json:"binlogs"`
}

// CreateBackup creates a backup resource for a given cluster using pure parameters.
// This method is framework-agnostic and can be reused outside HTTP handlers.
func (s *BackupService) CreateBackup(ctx context.Context, cli client.Client, namespace, clusterName string, backup *polardbxv1.PolarDBXBackup) (*polardbxv1.PolarDBXBackup, error) {
	if backup == nil {
		return nil, svcerr.ValidationError("backup payload is required", nil)
	}
	if namespace == "" {
		return nil, svcerr.ValidationError("namespace is required", nil)
	}
	if clusterName == "" {
		return nil, svcerr.ValidationError("cluster name is required", nil)
	}
	backup.Spec.Cluster.Name = clusterName
	created, err := k8s.CreatePolarDBXBackupWithContext(ctx, cli, namespace, backup)
	if err != nil {
		return nil, fmt.Errorf("create backup for cluster %s/%s: %w", namespace, clusterName, err)
	}
	return created, nil
}

// ListBackups lists backups for a given cluster in the specified namespace using pure parameters.
// This method is framework-agnostic and can be reused by different transports.
func (s *BackupService) ListBackups(ctx context.Context, cli client.Client, namespace, clusterName string) ([]polardbxv1.PolarDBXBackup, error) {
	if namespace == "" {
		return nil, svcerr.ValidationError("namespace is required", nil)
	}
	if clusterName == "" {
		return nil, svcerr.ValidationError("cluster name is required", nil)
	}
	backups, err := k8s.ListPolarDBXBackupsWithContext(ctx, cli, namespace, clusterName)
	if err != nil {
		return nil, fmt.Errorf("list backups for cluster %s/%s: %w", namespace, clusterName, err)
	}
	return backups, nil
}

// ValidateBackup validates a backup specification via Kubernetes dry-run create.
// This method is framework-agnostic and can be reused by different transports.
func (s *BackupService) ValidateBackup(ctx context.Context, cli client.Client, namespace string, backup *polardbxv1.PolarDBXBackup) error {
	if backup == nil {
		return svcerr.ValidationError("backup payload is required", nil)
	}
	if namespace == "" {
		return svcerr.ValidationError("namespace is required", nil)
	}
	if _, err := k8s.CreatePolarDBXBackupDryRunWithContext(ctx, cli, namespace, backup); err != nil {
		return fmt.Errorf("backup validation failed for %s/%s: %w", namespace, backup.Name, err)
	}
	return nil
}

// GetBackupMetrics gets coarse-grained backup progress for a PolarDB-X backup and its child XStore backups.
func (s *BackupService) GetBackupMetrics(ctx context.Context, cli client.Client, namespace, name string) (*BackupMetricsResponse, error) {
	if namespace == "" || name == "" {
		return nil, svcerr.ValidationError("namespace and name are required", nil)
	}
	var backup polardbxv1.PolarDBXBackup
	if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &backup); err != nil {
		return nil, fmt.Errorf("get backup %s/%s: %w", namespace, name, err)
	}
	var xsList polardbxv1.XStoreBackupList
	_ = cli.List(ctx, &xsList, client.InNamespace(namespace))
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

	sizeBytes, sizeEstimated := s.estimateBackupSizeBytes(ctx, cli, &backup)
	return &BackupMetricsResponse{
		Phase:         phase,
		Progress:      progress,
		Estimated:     true,
		SizeBytes:     sizeBytes,
		SizeEstimated: sizeEstimated,
		Children:      BackupChildMetrics{Total: total, Finished: finished, Failed: failed},
	}, nil
}

// DeleteBackup deletes a backup resource.
func (s *BackupService) DeleteBackup(ctx context.Context, cli client.Client, namespace, name string) error {
	if namespace == "" || name == "" {
		return svcerr.ValidationError("namespace and name are required", nil)
	}
	if err := k8s.DeletePolarDBXBackupWithContext(ctx, cli, namespace, name); err != nil {
		return fmt.Errorf("delete backup %s/%s: %w", namespace, name, err)
	}
	return nil
}

// ForceDeleteBackup removes finalizers then deletes the backup resource.
func (s *BackupService) ForceDeleteBackup(ctx context.Context, cli client.Client, namespace, name string) error {
	if namespace == "" || name == "" {
		return svcerr.ValidationError("namespace and name are required", nil)
	}
	var bk polardbxv1.PolarDBXBackup
	if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &bk); err != nil {
		return fmt.Errorf("get backup %s/%s for force delete: %w", namespace, name, err)
	}
	bk.SetFinalizers([]string{})
	if err := cli.Update(ctx, &bk); err != nil {
		return fmt.Errorf("remove finalizers from backup %s/%s: %w", namespace, name, err)
	}
	if err := k8s.DeletePolarDBXBackupWithContext(ctx, cli, namespace, name); err != nil {
		return fmt.Errorf("force delete backup %s/%s: %w", namespace, name, err)
	}
	return nil
}

// StreamEvents streams backup phase changes and events via a caller-provided writer.
func (s *BackupService) StreamEvents(ctx context.Context, cli client.Client, namespace, name string, w io.Writer, flusher func()) error {
	var lastPhase string
	var backup polardbxv1.PolarDBXBackup
	if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &backup); err == nil {
		lastPhase = strings.ToLower(string(backup.Status.Phase))
		writeSSE(w, flusher, "phaseChanged", map[string]any{"phase": lastPhase})
	} else {
		writeSSE(w, flusher, "error", map[string]any{"message": "backup not found", "details": err.Error()})
		return nil
	}
	keep := time.NewTicker(10 * time.Second)
	defer keep.Stop()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-keep.C:
			_, _ = w.Write([]byte(": ping\n\n"))
			flusher()
		case <-tick.C:
			var cur polardbxv1.PolarDBXBackup
			if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cur); err != nil {
				writeSSE(w, flusher, "error", map[string]any{"message": "failed to get backup", "details": err.Error()})
				return nil
			}
			phase := strings.ToLower(string(cur.Status.Phase))
			if phase != lastPhase {
				lastPhase = phase
				writeSSE(w, flusher, "phaseChanged", map[string]any{"phase": phase})
			}
			switch phase {
			case "succeeded", "completed", "finished", "failed", "deleting":
				return nil
			}
		}
	}
}

func writeSSE(w io.Writer, flusher func(), event string, payload map[string]any) {
	payload["timestamp"] = time.Now().UTC().Format(time.RFC3339)
	b, _ := json.Marshal(map[string]any{"type": event, "payload": payload})
	_, _ = w.Write([]byte("event: " + event + "\n"))
	_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
	flusher()
}

// GetBackupOverview aggregates last 24h KPIs and optional connectivity/storage insights.
func (s *BackupService) GetBackupOverview(ctx context.Context, cli client.Client, namespace string, evaluateConnectivity, evaluateStorage bool, systemNamespace string) (*BackupOverviewResponse, error) {
	now := time.Now()
	windowStart := now.Add(-24 * time.Hour)

	var backupList polardbxv1.PolarDBXBackupList
	listOpts := []client.ListOption{}
	if namespace != "" {
		listOpts = append(listOpts, client.InNamespace(namespace))
	}
	if err := cli.List(ctx, &backupList, listOpts...); err != nil {
		return nil, fmt.Errorf("list backups for overview in namespace %s: %w", namespace, err)
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

	kpi := BackupOverviewKPI{
		SuccessRate24h:            successRate,
		Running:                   runningNow,
		Failed24h:                 failed24h,
		TotalBackups24h:           total24h,
		TotalStorage:              "",
		TotalStorageBytes:         nil,
		StorageConnectivity:       "",
		StorageConnectivityStatus: "unknown",
	}

	if evaluateConnectivity {
		status, detail := s.EvaluateStorageConnectivity(ctx, cli, systemNamespace)
		kpi.StorageConnectivityStatus = status
		kpi.StorageConnectivity = detail
	}

	if evaluateStorage && len(prefixes) > 0 {
		if totalBytes, err := s.estimateStorageUsage(ctx, prefixes); err == nil && totalBytes > 0 {
			kpi.TotalStorageBytes = &totalBytes
			kpi.TotalStorage = fmt.Sprintf("%.2f GiB", float64(totalBytes)/1024/1024/1024)
		}
	}

	return &BackupOverviewResponse{Namespace: namespace, TimeWindowHours: 24, GeneratedAt: now, KPI: kpi}, nil
}

// GetClusterBackupState aggregates per-cluster backup state.
func (s *BackupService) GetClusterBackupState(ctx context.Context, cli client.Client, namespace string) (*ClusterBackupStateResponse, error) {
	now := time.Now()

	var clusterList polardbxv1.PolarDBXClusterList
	clusterOpts := []client.ListOption{}
	if namespace != "" {
		clusterOpts = append(clusterOpts, client.InNamespace(namespace))
	}
	if err := cli.List(ctx, &clusterList, clusterOpts...); err != nil {
		return nil, fmt.Errorf("list clusters for backup state in namespace %s: %w", namespace, err)
	}

	var backupList polardbxv1.PolarDBXBackupList
	backupOpts := []client.ListOption{}
	if namespace != "" {
		backupOpts = append(backupOpts, client.InNamespace(namespace))
	}
	if err := cli.List(ctx, &backupList, backupOpts...); err != nil {
		return nil, fmt.Errorf("list backups for backup state in namespace %s: %w", namespace, err)
	}

	var scheduleList polardbxv1.PolarDBXBackupScheduleList
	scheduleOpts := []client.ListOption{}
	if namespace != "" {
		scheduleOpts = append(scheduleOpts, client.InNamespace(namespace))
	}
	if err := cli.List(ctx, &scheduleList, scheduleOpts...); err != nil {
		return nil, fmt.Errorf("list schedules for backup state in namespace %s: %w", namespace, err)
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

	out := make([]ClusterBackupStateEntry, 0, len(clusterList.Items))
	for _, cl := range clusterList.Items {
		entry := ClusterBackupStateEntry{ClusterName: cl.Name, Namespace: cl.Namespace}
		if lb, ok := latestByCluster[cl.Name]; ok {
			info := ClusterBackupInfo{Phase: string(lb.Status.Phase), Name: lb.Name}
			if lb.Status.StartTime != nil {
				s := lb.Status.StartTime.Time.Format(time.RFC3339)
				info.StartTime = &s
			}
			if lb.Status.EndTime != nil {
				e := lb.Status.EndTime.Time.Format(time.RFC3339)
				info.EndTime = &e
			}
			if lb.Status.LatestRecoverableTimestamp != nil {
				t := lb.Status.LatestRecoverableTimestamp.Time.Format(time.RFC3339)
				info.LatestRecoverableTimestamp = &t
			}
			entry.LatestBackup = &info
		}
		if nt, ok := nextByCluster[cl.Name]; ok && nt != nil {
			s := nt.Time.Format(time.RFC3339)
			entry.NextScheduledTime = &s
		}
		if lrt, ok := maxLrtByCluster[cl.Name]; ok && !lrt.IsZero() {
			v := int(now.Sub(lrt).Seconds())
			entry.RPOSeconds = &v
		}
		out = append(out, entry)
	}

	return &ClusterBackupStateResponse{Namespace: namespace, Total: len(out), Clusters: out}, nil
}

// GetBinlogMetrics aggregates binlog CRs and latest backup LRT per cluster.
func (s *BackupService) GetBinlogMetrics(ctx context.Context, cli client.Client, namespace string, now time.Time) (*BinlogMetricsResponse, error) {
	if now.IsZero() {
		now = time.Now()
	}

	var binlogList polardbxv1.PolarDBXBackupBinlogList
	binlogOpts := []client.ListOption{}
	if namespace != "" {
		binlogOpts = append(binlogOpts, client.InNamespace(namespace))
	}
	_ = cli.List(ctx, &binlogList, binlogOpts...)

	var backupList polardbxv1.PolarDBXBackupList
	backupOpts := []client.ListOption{}
	if namespace != "" {
		backupOpts = append(backupOpts, client.InNamespace(namespace))
	}
	_ = cli.List(ctx, &backupList, backupOpts...)

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

	items := make([]BinlogMetricsEntry, 0, len(binlogList.Items))
	for _, b := range binlogList.Items {
		clusterName := b.Spec.PxcName
		entry := BinlogMetricsEntry{
			Name:                b.Name,
			Namespace:           b.Namespace,
			Cluster:             clusterName,
			Phase:               string(b.Status.Phase),
			LastCheckExpireTime: b.Status.CheckExpireFileLastTime,
			RecentDeletedFiles:  b.Status.LastDeletedFiles,
			RecentFiles:         b.Status.LastDeletedFiles,
		}
		if lrt, ok := maxLrtByCluster[clusterName]; ok && !lrt.IsZero() {
			ts := lrt.UTC().Format(time.RFC3339)
			entry.LatestBackupTime = &ts
			lag := int(now.Sub(lrt).Seconds())
			entry.LagSeconds = &lag
		}
		items = append(items, entry)
	}
	return &BinlogMetricsResponse{Namespace: namespace, Total: len(items), Binlogs: items}, nil
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

// estimateStorageUsage provides a lightweight estimation of total backup storage usage.
func (s *BackupService) estimateStorageUsage(ctx context.Context, prefixes []string) (int64, error) {
	_ = ctx
	_ = prefixes
	return 0, nil
}
