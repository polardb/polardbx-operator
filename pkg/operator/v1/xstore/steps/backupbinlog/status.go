package backupbinlog

import (
	"errors"
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

func UpdatePhaseTemplate(phase xstorev1.XStoreBackupBinlogPhase, requeue ...bool) control.BindFunc {
	return NewStepBinder("UpdatePhaseTo"+string(phase),
		func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error) {
			xstoreBackupBinlog := rc.MustGetXStoreBackupBinlog()
			xstoreBackupBinlog.Status.Phase = phase
			rc.MarkXStoreChanged()
			if len(requeue) == 0 || !requeue[0] {
				return flow.Pass()
			} else {
				return flow.Retry("Phase updated!", "target-phase", phase)
			}
		})
}

var PersistentBackupBinlog = NewStepBinder("PersistentStatusChanges",
	func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error) {
		if rc.IsXStoreChanged() {
			if err := rc.UpdateXStoreBackupBinlog(); err != nil {
				return flow.Error(err, "Unable to persistent polardbx backup binlog.")
			}
			return flow.Continue("Succeeds to persistent polardbx backup binlog.")
		}
		return flow.Continue("Object not changed.")
	})

var InitFromXStore = NewStepBinder("InitFromXStore",
	func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error) {
		backupBinlog := rc.MustGetXStoreBackupBinlog()
		xstore := rc.MustGetXStore()
		xstore.SetAnnotations(k8shelper.PatchAnnotations(xstore.GetAnnotations(), map[string]string{
			meta.AnnotationBackupBinlog: "true",
		}))
		err := rc.Client().Update(rc.Context(), xstore)
		if err != nil {
			return flow.RetryErr(err, "failed to update xstore ", "xstore name", xstore.Name)
		}
		backupBinlog.Spec.XStoreName = xstore.Name
		backupBinlog.Spec.XStoreUid = string(xstore.UID)
		labels := backupBinlog.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels = k8shelper.PatchLabels(labels, map[string]string{
			meta.LabelName: backupBinlog.Spec.XStoreName,
			meta.LabelUid:  backupBinlog.Spec.XStoreUid,
		})
		backupBinlog.SetLabels(labels)
		rc.MarkXStoreChanged()
		return flow.Continue("InitFromXStore.")
	})

var RunningRoute = NewStepBinder("RunningRoute",
	func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error) {
		backupBinlog := rc.MustGetXStoreBackupBinlog()
		now := time.Now()
		checkInterval, err := rc.XStoreContext().Config().Backup().CheckBinlogExpiredFileInterval()
		if err != nil {
			flow.Logger().Error(err, "failed to get check backup binlog interval")
			return flow.Pass()
		}

		if now.Unix()-int64(backupBinlog.Status.CheckExpireFileLastTime) > int64(checkInterval.Seconds()) {
			backupBinlog.Status.Phase = xstorev1.XStoreBackupBinlogPhaseCheckExpiredFile
			rc.MarkXStoreChanged()
		}

		return flow.RetryAfter(5*time.Second, "RunningRoute.")
	})

var TryDeleteExpiredFiles = NewStepBinder("TryDeleteExpiredFiles",
	func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error) {
		hpfsClient, err := rc.XStoreContext().GetHpfsClient()
		if err != nil {
			return flow.RetryErr(err, "failed to get hpfs client")
		}
		now := time.Now()
		backupBinlog := rc.MustGetXStoreBackupBinlog()
		seconds := backupBinlog.Spec.RemoteExpireLogHours.IntValue() * 3600
		if !backupBinlog.DeletionTimestamp.IsZero() {
			seconds = -3600
		}
		rep, err := hpfsClient.DeleteBinlogFilesBefore(rc.Context(), &hpfs.DeleteBinlogFilesBeforeRequest{
			Namespace: backupBinlog.Namespace,
			PxcName:   backupBinlog.Spec.XStoreName,
			PxcUid:    backupBinlog.Spec.XStoreUid,
			SinkName:  backupBinlog.Spec.StorageProvider.Sink,
			SinkType:  string(backupBinlog.Spec.StorageProvider.StorageName),
			UnixTime:  now.Unix() - int64(seconds),
		})
		if err != nil {
			flow.Logger().Error(err, "failed to DeleteBinlogFilesBefore")
		}
		if rep.Status.Code != hpfs.Status_OK {
			flow.Logger().Error(errors.New(rep.GetStatus().String()), "failed response")
		}
		if rep.Status.Code == hpfs.Status_OK {
			flow.Logger().Info("delete files", "files", rep.GetDeletedFiles())
			backupBinlog.Status.LastDeletedFiles = rep.GetDeletedFiles()
		}
		backupBinlog.Status.CheckExpireFileLastTime = uint64(now.Unix())

		return flow.Continue("TryDeleteExpiredFiles.")
	})

var CleanFromXStore = NewStepBinder("CleanFromXStore",
	func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		annotation := xstore.GetAnnotations()
		if _, ok := annotation[meta.AnnotationBackupBinlog]; !ok {
			return flow.Pass()
		}
		delete(annotation, meta.AnnotationBackupBinlog)
		xstore.SetAnnotations(annotation)
		err := rc.Client().Update(rc.Context(), xstore)
		if err != nil {
			return flow.RetryErr(err, "failed to update xstore ", "xstore name", xstore.Name)
		}
		rc.MarkXStoreChanged()
		return flow.Continue("CleanFromXStore.")
	})

var CloseBackupBinlog = NewStepBinder("CloseBackupBinlog",
	func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error) {
		hpfsClient, err := rc.XStoreContext().GetHpfsClient()
		if err != nil {
			return flow.RetryErr(err, "failed to get hpfs client")
		}
		xstore := rc.GetActiveXStore()
		if xstore != nil {
			for _, boundVolume := range xstore.Status.BoundVolumes {
				workDir := filepath.Join(boundVolume.HostPath, "log")
				if xstore.Spec.Config.Dynamic.LogDataSeparation {
					workDir = filepath.Join(boundVolume.LogHostPath, "log")
				}
				resp, err := hpfsClient.CloseBackupBinlog(rc.Context(), &hpfs.CloseBackupBinlogRequest{
					Host:   &hpfs.Host{NodeName: boundVolume.Host},
					LogDir: workDir,
				})
				if err != nil {
					return flow.RetryErr(err, "failed to close backup binlog", "logDir", workDir, "nodeName", boundVolume.Host)
				}
				if resp.Status.Code != hpfs.Status_OK {
					return flow.RetryErr(errors.New("CloseBackupBinlogRequest status not ok: "+resp.Status.Code.String()), "logDir", workDir, "nodeName", boundVolume.Host)
				}
			}
		}
		return flow.Continue("CloseBackupBinlog.")
	})

var ConfirmRemoteEmptyFiles = NewStepBinder("ConfirmRemoteEmptyFiles",
	func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error) {
		hpfsClient, err := rc.XStoreContext().GetHpfsClient()
		if err != nil {
			return flow.RetryErr(err, "failed to get hpfs client")
		}
		backupBinlog := rc.MustGetXStoreBackupBinlog()
		rep, err := hpfsClient.ListRemoteBinlogList(rc.Context(), &hpfs.ListRemoteBinlogListRequest{
			Namespace:  backupBinlog.Namespace,
			XStoreName: backupBinlog.Spec.XStoreName,
			XStoreUid:  backupBinlog.Spec.XStoreUid,
			SinkName:   backupBinlog.Spec.StorageProvider.Sink,
			SinkType:   string(backupBinlog.Spec.StorageProvider.StorageName),
		})
		if err == nil && len(rep.Files) > 0 {
			return flow.RetryErr(err, "file exists")
		}
		return flow.Continue("ConfirmRemoteEmptyFiles.")

	})

func WhenXStoreExist(binders ...control.BindFunc) control.BindFunc {
	return NewStepIfBinder("xstoreExist",
		func(rc *xstorev1reconcile.BackupBinlogContext, log logr.Logger) (bool, error) {
			backupBinlog := rc.MustGetXStoreBackupBinlog()
			xstore, err := rc.GetXStore()
			if apierrors.IsNotFound(err) || string(xstore.UID) != backupBinlog.Spec.XStoreUid {
				return false, nil
			}
			return true, nil
		},
		binders...,
	)
}

func WhenDeleting(binders ...control.BindFunc) control.BindFunc {
	return NewStepIfBinder("Deleted",
		func(rc *xstorev1reconcile.BackupBinlogContext, log logr.Logger) (bool, error) {
			backupBinlog := rc.MustGetXStoreBackupBinlog()
			if backupBinlog.Status.Phase == xstorev1.XStoreBackupBinlogPhaseDeleting {
				return false, nil
			}
			deleting := !backupBinlog.DeletionTimestamp.IsZero()
			return deleting, nil
		},
		binders...,
	)
}

var AddFinalizer = NewStepBinder("AddFinalizer",
	func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error) {
		backupBinlog := rc.MustGetXStoreBackupBinlog()
		if controllerutil.ContainsFinalizer(backupBinlog, meta.Finalizer) {
			return flow.Pass()
		}
		controllerutil.AddFinalizer(backupBinlog, meta.Finalizer)
		rc.MarkXStoreChanged()
		return flow.Continue("Add finalizer.")
	})

var RemoveFinalizer = NewStepBinder("RemoveFinalizer",
	func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error) {
		backupBinlog := rc.MustGetXStoreBackupBinlog()
		if !controllerutil.ContainsFinalizer(backupBinlog, meta.Finalizer) {
			return flow.Pass()
		}
		controllerutil.RemoveFinalizer(backupBinlog, meta.Finalizer)
		rc.MarkXStoreChanged()
		return flow.Continue("Remove finalizer.")
	})
