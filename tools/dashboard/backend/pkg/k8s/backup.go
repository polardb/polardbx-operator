package k8s

import (
	"context"
	"log"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ---- PolarDBXBackup (and dry-run) ----

// Deprecated: Use ListPolarDBXBackupsWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListPolarDBXBackups(c client.Client, namespace, clusterName string) ([]polardbxv1.PolarDBXBackup, error) {
	log.Printf("WARNING: Using deprecated ListPolarDBXBackups without context. Please migrate to ListPolarDBXBackupsWithContext.")
	return ListPolarDBXBackupsWithContext(context.Background(), c, namespace, clusterName)
}

// Deprecated: Use CreatePolarDBXBackupWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreatePolarDBXBackup(c client.Client, namespace string, backup *polardbxv1.PolarDBXBackup) (*polardbxv1.PolarDBXBackup, error) {
	log.Printf("WARNING: Using deprecated CreatePolarDBXBackup without context. Please migrate to CreatePolarDBXBackupWithContext.")
	return CreatePolarDBXBackupWithContext(context.Background(), c, namespace, backup)
}

// Deprecated: Use CreatePolarDBXBackupDryRunWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreatePolarDBXBackupDryRun(c client.Client, namespace string, backup *polardbxv1.PolarDBXBackup) (*polardbxv1.PolarDBXBackup, error) {
	log.Printf("WARNING: Using deprecated CreatePolarDBXBackupDryRun without context. Please migrate to CreatePolarDBXBackupDryRunWithContext.")
	return CreatePolarDBXBackupDryRunWithContext(context.Background(), c, namespace, backup)
}

// Deprecated: Use DeletePolarDBXBackupWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeletePolarDBXBackup(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeletePolarDBXBackup without context. Please migrate to DeletePolarDBXBackupWithContext.")
	return DeletePolarDBXBackupWithContext(context.Background(), c, namespace, name)
}

func ListPolarDBXBackupsWithContext(ctx context.Context, c client.Client, namespace, clusterName string) ([]polardbxv1.PolarDBXBackup, error) {
	var list polardbxv1.PolarDBXBackupList
	opts := []client.ListOption{client.InNamespace(namespace), client.MatchingLabels{"polardbx/name": clusterName}}
	if err := c.List(ctx, &list, opts...); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreatePolarDBXBackupWithContext(ctx context.Context, c client.Client, namespace string, backup *polardbxv1.PolarDBXBackup) (*polardbxv1.PolarDBXBackup, error) {
	if backup.Namespace == "" {
		backup.Namespace = namespace
	}
	if err := c.Create(ctx, backup); err != nil {
		return nil, err
	}
	return backup, nil
}

func CreatePolarDBXBackupDryRunWithContext(ctx context.Context, c client.Client, namespace string, backup *polardbxv1.PolarDBXBackup) (*polardbxv1.PolarDBXBackup, error) {
	if backup.Namespace == "" {
		backup.Namespace = namespace
	}
	opts := &client.CreateOptions{DryRun: []string{metav1.DryRunAll}}
	if err := c.Create(ctx, backup, opts); err != nil {
		return nil, err
	}
	return backup, nil
}

func DeletePolarDBXBackupWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.PolarDBXBackup{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// ---- PolarDBXBackupSchedule ----

// Deprecated: Use ListPolarDBXBackupSchedulesWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListPolarDBXBackupSchedules(c client.Client, namespace string) ([]polardbxv1.PolarDBXBackupSchedule, error) {
	log.Printf("WARNING: Using deprecated ListPolarDBXBackupSchedules without context. Please migrate to ListPolarDBXBackupSchedulesWithContext.")
	return ListPolarDBXBackupSchedulesWithContext(context.Background(), c, namespace)
}

// Deprecated: Use CreatePolarDBXBackupScheduleWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreatePolarDBXBackupSchedule(c client.Client, namespace string, schedule *polardbxv1.PolarDBXBackupSchedule) (*polardbxv1.PolarDBXBackupSchedule, error) {
	log.Printf("WARNING: Using deprecated CreatePolarDBXBackupSchedule without context. Please migrate to CreatePolarDBXBackupScheduleWithContext.")
	return CreatePolarDBXBackupScheduleWithContext(context.Background(), c, namespace, schedule)
}

// Deprecated: Use GetPolarDBXBackupScheduleWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetPolarDBXBackupSchedule(c client.Client, namespace, name string) (*polardbxv1.PolarDBXBackupSchedule, error) {
	log.Printf("WARNING: Using deprecated GetPolarDBXBackupSchedule without context. Please migrate to GetPolarDBXBackupScheduleWithContext.")
	return GetPolarDBXBackupScheduleWithContext(context.Background(), c, namespace, name)
}

// Deprecated: Use UpdatePolarDBXBackupScheduleWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdatePolarDBXBackupSchedule(c client.Client, namespace string, schedule *polardbxv1.PolarDBXBackupSchedule) (*polardbxv1.PolarDBXBackupSchedule, error) {
	log.Printf("WARNING: Using deprecated UpdatePolarDBXBackupSchedule without context. Please migrate to UpdatePolarDBXBackupScheduleWithContext.")
	return UpdatePolarDBXBackupScheduleWithContext(context.Background(), c, namespace, schedule)
}

// Deprecated: Use DeletePolarDBXBackupScheduleWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeletePolarDBXBackupSchedule(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeletePolarDBXBackupSchedule without context. Please migrate to DeletePolarDBXBackupScheduleWithContext.")
	return DeletePolarDBXBackupScheduleWithContext(context.Background(), c, namespace, name)
}

func ListPolarDBXBackupSchedulesWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXBackupSchedule, error) {
	var list polardbxv1.PolarDBXBackupScheduleList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreatePolarDBXBackupScheduleWithContext(ctx context.Context, c client.Client, namespace string, schedule *polardbxv1.PolarDBXBackupSchedule) (*polardbxv1.PolarDBXBackupSchedule, error) {
	if schedule.Namespace == "" {
		schedule.Namespace = namespace
	}
	if err := c.Create(ctx, schedule); err != nil {
		return nil, err
	}
	return schedule, nil
}

func GetPolarDBXBackupScheduleWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXBackupSchedule, error) {
	var item polardbxv1.PolarDBXBackupSchedule
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &item); err != nil {
		return nil, err
	}
	return &item, nil
}

func UpdatePolarDBXBackupScheduleWithContext(ctx context.Context, c client.Client, namespace string, schedule *polardbxv1.PolarDBXBackupSchedule) (*polardbxv1.PolarDBXBackupSchedule, error) {
	if err := c.Update(ctx, schedule); err != nil {
		return nil, err
	}
	return schedule, nil
}

func DeletePolarDBXBackupScheduleWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.PolarDBXBackupSchedule{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// ---- PolarDBXBackupBinlog ----

// Deprecated: Use ListPolarDBXBackupBinlogsWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func ListPolarDBXBackupBinlogs(c client.Client, namespace string) ([]polardbxv1.PolarDBXBackupBinlog, error) {
	log.Printf("WARNING: Using deprecated ListPolarDBXBackupBinlogs without context. Please migrate to ListPolarDBXBackupBinlogsWithContext.")
	return ListPolarDBXBackupBinlogsWithContext(context.Background(), c, namespace)
}

// Deprecated: Use CreatePolarDBXBackupBinlogWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func CreatePolarDBXBackupBinlog(c client.Client, namespace string, binlog *polardbxv1.PolarDBXBackupBinlog) (*polardbxv1.PolarDBXBackupBinlog, error) {
	log.Printf("WARNING: Using deprecated CreatePolarDBXBackupBinlog without context. Please migrate to CreatePolarDBXBackupBinlogWithContext.")
	return CreatePolarDBXBackupBinlogWithContext(context.Background(), c, namespace, binlog)
}

// Deprecated: Use GetPolarDBXBackupBinlogWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func GetPolarDBXBackupBinlog(c client.Client, namespace, name string) (*polardbxv1.PolarDBXBackupBinlog, error) {
	log.Printf("WARNING: Using deprecated GetPolarDBXBackupBinlog without context. Please migrate to GetPolarDBXBackupBinlogWithContext.")
	return GetPolarDBXBackupBinlogWithContext(context.Background(), c, namespace, name)
}

// Deprecated: Use UpdatePolarDBXBackupBinlogWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func UpdatePolarDBXBackupBinlog(c client.Client, namespace string, binlog *polardbxv1.PolarDBXBackupBinlog) (*polardbxv1.PolarDBXBackupBinlog, error) {
	log.Printf("WARNING: Using deprecated UpdatePolarDBXBackupBinlog without context. Please migrate to UpdatePolarDBXBackupBinlogWithContext.")
	return UpdatePolarDBXBackupBinlogWithContext(context.Background(), c, namespace, binlog)
}

// Deprecated: Use DeletePolarDBXBackupBinlogWithContext for better context control.
// This function uses context.Background() which cannot be cancelled or timed out.
func DeletePolarDBXBackupBinlog(c client.Client, namespace, name string) error {
	log.Printf("WARNING: Using deprecated DeletePolarDBXBackupBinlog without context. Please migrate to DeletePolarDBXBackupBinlogWithContext.")
	return DeletePolarDBXBackupBinlogWithContext(context.Background(), c, namespace, name)
}

func ListPolarDBXBackupBinlogsWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXBackupBinlog, error) {
	var list polardbxv1.PolarDBXBackupBinlogList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreatePolarDBXBackupBinlogWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.PolarDBXBackupBinlog) (*polardbxv1.PolarDBXBackupBinlog, error) {
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := c.Create(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func GetPolarDBXBackupBinlogWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXBackupBinlog, error) {
	var out polardbxv1.PolarDBXBackupBinlog
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func UpdatePolarDBXBackupBinlogWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.PolarDBXBackupBinlog) (*polardbxv1.PolarDBXBackupBinlog, error) {
	if err := c.Update(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func DeletePolarDBXBackupBinlogWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	binlog := &polardbxv1.PolarDBXBackupBinlog{}
	binlog.Name = name
	binlog.Namespace = namespace
	return c.Delete(ctx, binlog)
}
