package backupbinlog

import (
	"errors"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

var TryDeleteExpiredFiles = polardbxv1reconcile.NewStepBinder("TryDeleteExpiredFiles", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	hpfsClient, err := GetHpfsClient(rc)
	if err != nil {
		return flow.RetryErr(err, "failed to get hpfs client")
	}
	now := time.Now()
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	seconds := backupBinlog.Spec.RemoteExpireLogHours.IntValue() * 3600
	if !backupBinlog.DeletionTimestamp.IsZero() {
		seconds = -3600
	}
	rep, err := hpfsClient.DeleteBinlogFilesBefore(rc.Context(), &hpfs.DeleteBinlogFilesBeforeRequest{
		Namespace: backupBinlog.Namespace,
		PxcName:   backupBinlog.Spec.PxcName,
		PxcUid:    backupBinlog.Spec.PxcUid,
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

var ConfirmRemoteEmptyFiles = polardbxv1reconcile.NewStepBinder("TryDeleteExpiredFiles", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	hpfsClient, err := GetHpfsClient(rc)
	if err != nil {
		return flow.RetryErr(err, "failed to get hpfs client")
	}
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	rep, err := hpfsClient.ListRemoteBinlogList(rc.Context(), &hpfs.ListRemoteBinlogListRequest{
		Namespace: backupBinlog.Namespace,
		PxcName:   backupBinlog.Spec.PxcName,
		PxcUid:    backupBinlog.Spec.PxcUid,
		SinkName:  backupBinlog.Spec.StorageProvider.Sink,
		SinkType:  string(backupBinlog.Spec.StorageProvider.StorageName),
	})
	if err == nil && len(rep.Files) > 0 {
		return flow.RetryErr(err, "file exists")
	}
	return flow.Continue("ConfirmRemoteEmptyFiles.")
})
