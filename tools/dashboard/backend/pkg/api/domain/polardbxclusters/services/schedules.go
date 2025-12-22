package services

import (
	"context"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	cronv3 "github.com/robfig/cron/v3"
	"sigs.k8s.io/controller-runtime/pkg/client"

	svcerr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/k8s"
)

// BackupScheduleService encapsulates BackupSchedule related orchestration (pure service).
type BackupScheduleService struct{}

func NewBackupScheduleService() *BackupScheduleService { return &BackupScheduleService{} }

// ListSchedules lists backup schedules in a namespace.
func (s *BackupScheduleService) ListSchedules(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.PolarDBXBackupSchedule, error) {
	if namespace == "" {
		return nil, svcerr.ValidationError("namespace is required", nil)
	}
	items, err := k8s.ListPolarDBXBackupSchedulesWithContext(ctx, cli, namespace)
	if err != nil {
		return nil, err
	}
	return items, nil
}

// CreateSchedule creates a new backup schedule.
func (s *BackupScheduleService) CreateSchedule(ctx context.Context, cli client.Client, namespace string, body *polardbxv1.PolarDBXBackupSchedule) (*polardbxv1.PolarDBXBackupSchedule, error) {
	if body == nil {
		return nil, svcerr.ValidationError("schedule payload is required", nil)
	}
	if namespace == "" {
		return nil, svcerr.ValidationError("namespace is required", nil)
	}
	body.Namespace = namespace
	return k8s.CreatePolarDBXBackupScheduleWithContext(ctx, cli, namespace, body)
}

// GetSchedule gets a specific backup schedule.
func (s *BackupScheduleService) GetSchedule(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.PolarDBXBackupSchedule, error) {
	if namespace == "" || name == "" {
		return nil, svcerr.ValidationError("namespace and name are required", nil)
	}
	return k8s.GetPolarDBXBackupScheduleWithContext(ctx, cli, namespace, name)
}

// UpdateSchedule updates an existing backup schedule.
func (s *BackupScheduleService) UpdateSchedule(ctx context.Context, cli client.Client, namespace string, body *polardbxv1.PolarDBXBackupSchedule) (*polardbxv1.PolarDBXBackupSchedule, error) {
	if body == nil {
		return nil, svcerr.ValidationError("schedule payload is required", nil)
	}
	if namespace == "" {
		return nil, svcerr.ValidationError("namespace is required", nil)
	}
	body.Namespace = namespace
	return k8s.UpdatePolarDBXBackupScheduleWithContext(ctx, cli, namespace, body)
}

// DeleteSchedule deletes a backup schedule.
func (s *BackupScheduleService) DeleteSchedule(ctx context.Context, cli client.Client, namespace, name string) error {
	if namespace == "" || name == "" {
		return svcerr.ValidationError("namespace and name are required", nil)
	}
	return k8s.DeletePolarDBXBackupScheduleWithContext(ctx, cli, namespace, name)
}

// NextRunsResult represents next run calculation result for a schedule.
type NextRunsResult struct {
	Name        string `json:"name"`
	Namespace   string `json:"namespace"`
	Schedule    string `json:"schedule"`
	NextRunTime string `json:"nextRunTime"`
	ParseError  string `json:"parseError,omitempty"`
}

// GetNextRuns computes next run time per schedule.
func (s *BackupScheduleService) GetNextRuns(ctx context.Context, cli client.Client, namespace string, now time.Time) ([]NextRunsResult, error) {
	if now.IsZero() {
		now = time.Now()
	}
	var list polardbxv1.PolarDBXBackupScheduleList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := cli.List(ctx, &list, opts...); err != nil {
		return nil, err
	}
	items := make([]NextRunsResult, 0, len(list.Items))
	for _, it := range list.Items {
		entry := NextRunsResult{
			Name:      it.Name,
			Namespace: it.Namespace,
			Schedule:  it.Spec.Schedule,
		}
		if it.Status.NextBackupTime != nil && !it.Status.NextBackupTime.Time.IsZero() {
			entry.NextRunTime = it.Status.NextBackupTime.Time.Format(time.RFC3339)
		} else if it.Spec.Schedule != "" {
			if sch, err := cronv3.ParseStandard(it.Spec.Schedule); err == nil {
				next := sch.Next(now)
				entry.NextRunTime = next.UTC().Format(time.RFC3339)
			} else {
				entry.NextRunTime = ""
				entry.ParseError = err.Error()
			}
		} else {
			entry.NextRunTime = ""
			entry.ParseError = "empty schedule"
		}
		items = append(items, entry)
	}
	return items, nil
}
