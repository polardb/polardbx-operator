package schedule

import (
	"github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/factory"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"github.com/robfig/cron"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sort"
	"time"
)

var PersistPolarDBXBackupScheduleStatus = polardbxv1reconcile.NewStepBinder("PersistPolarDBXBackupScheduleStatus",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsPolarDBXBackupScheduleStatusChanged() {
			err := rc.UpdatePolarDBXBackupScheduleStatus()
			if err != nil {
				return flow.Error(err, "Unable to update PolarDBX backup schedule status.")
			}
			return flow.Continue("PolarDBX backup schedule status updated.")
		}
		return flow.Continue("PolarDBX backup schedule status has not been changed.")
	})

var CleanOutdatedBackupSet = polardbxv1reconcile.NewStepBinder("CleanOutdatedBackupSet",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backupSchedule := rc.MustGetPolarDBXBackupSchedule()
		if backupSchedule.Spec.MaxBackupCount == 0 {
			return flow.Continue("No limit on the count of backup set.")
		}

		backupList, err := rc.GetPolarDBXBackupListByScheduleName(backupSchedule.Name)
		if err != nil {
			return flow.Error(err, "Failed to get backup list.", "schedule name", backupSchedule.Name)
		}
		if backupSchedule.Spec.MaxBackupCount > len(backupList.Items) {
			return flow.Continue("No outdated backup set needs to be cleaned.")
		}

		backupItems := backupList.Items
		sort.Slice(backupItems, func(i, j int) bool {
			a, b := backupItems[i], backupItems[j]
			return a.CreationTimestamp.Before(&b.CreationTimestamp)
		})

		for i := 0; i < len(backupItems)-backupSchedule.Spec.MaxBackupCount; i++ {
			flow.Logger().Info("Delete outdated backup", "backup name", backupItems[i].Name)
			err := rc.Client().Delete(rc.Context(), &backupItems[i])
			if client.IgnoreNotFound(err) != nil {
				return flow.Error(err, "Failed to delete backup.", "backup name", backupItems[i].Name)
			}
		}

		return flow.Continue("Outdated backup set cleaned.")
	})

var CheckNextScheduleTime = polardbxv1reconcile.NewStepBinder("CheckNextScheduleTime",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backupSchedule := rc.MustGetPolarDBXBackupSchedule()

		// Parse schedule
		schedule, err := cron.ParseStandard(backupSchedule.Spec.Schedule)
		if err != nil {
			return flow.Error(err, "Parse schedule string failed.")
		}

		// Check whether it is time to perform backup
		var lastTime time.Time
		if backupSchedule.Status.LastBackupTime != nil {
			lastTime = backupSchedule.Status.LastBackupTime.Time
		} else {
			lastTime = backupSchedule.CreationTimestamp.Time
		}
		nextTime := schedule.Next(lastTime)
		currentTime := time.Now()

		// Ready for backup
		if nextTime.Before(currentTime) {
			return flow.Continue("It is high time for backup.")
		}

		// Wait until next backup
		backupSchedule.Status.NextBackupTime = &metav1.Time{Time: nextTime}
		return flow.RetryAfter(nextTime.Sub(time.Now()), "It is not the time for backup.", "next backup time", nextTime)
	})

var CheckUnderwayBackup = polardbxv1reconcile.NewStepBinder("CheckUnderwayBackup",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backupSchedule := rc.MustGetPolarDBXBackupSchedule()
		polardbxName := backupSchedule.Spec.BackupSpec.Cluster.Name
		backupList, err := rc.GetPolarDBXBackupListByPolarDBXName(polardbxName)
		if err != nil {
			return flow.Error(err, "Failed to get backup list", "PolarDBX name", polardbxName)
		}
		for _, backup := range backupList.Items {
			if backup.Status.Phase != v1.BackupFinished && backup.Status.Phase != v1.BackupFailed && backup.Status.Phase != v1.BackupDummy {
				return flow.RetryAfter(1*time.Minute, "Backup is still underway", "backup name", backup.Name)
			}
		}
		return flow.Continue("No backup is underway.")
	})

var DispatchBackupTask = polardbxv1reconcile.NewStepBinder("DispatchBackupTask",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backupSchedule := rc.MustGetPolarDBXBackupSchedule()

		// Perform backup
		objectFactory := factory.NewObjectFactory(rc)
		polardbxBackup, err := objectFactory.NewPolarDBXBackupBySchedule()
		if err != nil {
			return flow.RetryErr(err, "Failed to new backup.")
		}
		err = rc.Client().Create(rc.Context(), polardbxBackup)
		if err != nil {
			return flow.RetryErr(err, "Failed to create backup.")
		}
		flow.Logger().Info("New backup created", "backup", polardbxBackup.Name)

		// Record backup info
		backupSchedule.Status.LastBackupTime = &metav1.Time{Time: time.Now()}
		backupSchedule.Status.LastBackup = polardbxBackup.Name

		return flow.Continue("Backup task dispatched.")
	})
