package common

import (
	"errors"
	"fmt"
	v1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/polardbx"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var CleanRemoteBackupFiles = polardbxv1reconcile.NewStepBinder("CleanRemoteBackupFile",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		if backup.Spec.CleanPolicy == polardbx.CleanPolicyRetain ||
			(backup.Spec.CleanPolicy == polardbx.CleanPolicyOnFailure && backup.Status.Phase != v1.BackupFailed) {
			return flow.Continue("No need to clean remote backup files.")
		}

		client, err := rc.GetHpfsClient()
		if err != nil {
			return flow.Error(err, "Failed to get hpfs client.")
		}

		response, err := client.DeleteRemoteFile(rc.Context(), &hpfs.DeleteRemoteFileRequest{
			SinkType: string(backup.Spec.StorageProvider.StorageName),
			SinkName: backup.Spec.StorageProvider.Sink,
			Target: &hpfs.RemoteFsEndpoint{
				Path: backup.Status.BackupRootPath,
				Other: map[string]string{
					"recursive": "true",
				},
			},
		})
		if response.GetStatus().Code != hpfs.Status_OK {
			return flow.Error(errors.New("cleanup failure"),
				fmt.Sprintf("reponse status code: %s, message: %s",
					response.GetStatus().Code, response.GetStatus().Message))
		}

		return flow.Continue("Remote backup files cleaned.")
	})
