/*
Copyright 2021 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"bytes"
	"encoding/json"
	"errors"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/factory"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	polarxPath "github.com/alibaba/polardbx-operator/pkg/util/path"
	"github.com/google/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var PersistentStatus = polardbxv1reconcile.NewStepBinder("PersistentStatus",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsPolarDBXStatusChanged() {
			err := rc.UpdatePolarDBXStatus()
			if err != nil {
				return flow.Error(err, "Unable to persistent status.")
			}
		}
		return flow.Pass()
	},
)

var PersistentPolarDBXCluster = polardbxv1reconcile.NewStepBinder("PersistentPolarDBXCluster",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsPolarDBXChanged() {
			err := rc.UpdatePolarDBX()
			if err != nil {
				return flow.Error(err, "Unable to persistent polardbx.")
			}
		}
		return flow.Pass()
	},
)

var InitializeServiceName = polardbxv1reconcile.NewStepBinder("InitializeServiceName",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if len(polardbx.Spec.ServiceName) == 0 {
			polardbx.Spec.ServiceName = polardbx.Name
			rc.MarkPolarDBXChanged()
		}
		return flow.Pass()
	},
)

var InitializePolardbxLabel = polardbxv1reconcile.NewStepBinder("InitializePolardbxLabel",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if polardbx.Spec.Readonly {
			polardbx.SetLabels(
				k8shelper.PatchLabels(
					polardbx.Labels,
					map[string]string{
						polardbxmeta.LabelType:        polardbxmeta.TypeReadonly,
						polardbxmeta.LabelPrimaryName: polardbx.Spec.PrimaryCluster,
					},
				),
			)
		} else {
			polardbx.SetLabels(
				k8shelper.PatchLabels(
					polardbx.Labels,
					map[string]string{
						polardbxmeta.LabelType: polardbxmeta.TypeMaster,
					},
				),
			)
		}
		err := rc.Client().Update(rc.Context(), polardbx)

		if err != nil {
			return flow.Error(err, "Failed to init polardbx label.")
		}

		return flow.Pass()
	},
)

var CheckDNs = polardbxv1reconcile.NewStepBinder("CheckDNs",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		topology := polardbx.Status.SpecSnapshot.Topology
		replicas := int(topology.Nodes.DN.Replicas)

		dnStores, err := rc.GetDNMap()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}

		// Ensure DNs are sorted and their indexes are incremental
		// when phase is not in creating or restoring.
		lastIndex := 0
		for ; lastIndex < replicas; lastIndex++ {
			if _, ok := dnStores[lastIndex]; !ok {
				break
			}
		}
		if lastIndex != replicas && lastIndex != len(dnStores) {
			helper.TransferPhase(polardbx, polardbxv1polardbx.PhaseFailed)
			return flow.Retry("Found broken DN, transfer into failed.")
		}

		return flow.Pass()
	},
)

// helper function to download metadata backup from remote storage
func downloadMetadataBackup(rc *polardbxv1reconcile.Context) (*factory.MetadataBackup, error) {
	polardbx := rc.MustGetPolarDBX()
	filestreamClient, err := rc.GetFilestreamClient()
	if err != nil {
		return nil, errors.New("failed to get filestream client, error: " + err.Error())
	}
	filestreamAction, err := polardbxv1polardbx.NewBackupStorageFilestreamAction(polardbx.Spec.Restore.StorageProvider.StorageName)

	downloadActionMetadata := filestream.ActionMetadata{
		Action:    filestreamAction.Download,
		Sink:      polardbx.Spec.Restore.StorageProvider.Sink,
		RequestId: uuid.New().String(),
		Filename:  polarxPath.NewPathFromStringSequence(polardbx.Spec.Restore.From.BackupSetPath, "metadata"),
	}
	var downloadBuffer bytes.Buffer
	recvBytes, err := filestreamClient.Download(&downloadBuffer, downloadActionMetadata)
	if err != nil {
		return nil, errors.New("download metadata failed, error: " + err.Error())
	}
	if recvBytes == 0 {
		return nil, errors.New("no byte received, please check storage config and target path")
	}
	metadata := &factory.MetadataBackup{}
	err = json.Unmarshal(downloadBuffer.Bytes(), &metadata)
	if err != nil {
		return nil, errors.New("failed to parse metadata, error: " + err.Error())
	}
	return metadata, nil
}

// CreateDummyBackupObject creates dummy polardbx backup when BackupSetPath provided.
// The dummy backup object has only necessary information for restore.
var CreateDummyBackupObject = polardbxv1reconcile.NewStepBinder("CreateDummyBackupObject",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if polardbx.Spec.Restore.BackupSet != "" {
			return flow.Continue("Backup set is specified, no need to create dummy backup object")
		}
		if polardbx.Spec.Restore.From.BackupSetPath == "" {
			return flow.Continue("BackupSetPath is not specified, no need to create dummy backup object")
		}

		metadata, err := downloadMetadataBackup(rc)
		if err != nil {
			helper.TransferPhase(polardbx, polardbxv1polardbx.PhaseFailed)
			return flow.Error(err, "Failed to download metadata from backup set path",
				"path", polardbx.Spec.Restore.From.BackupSetPath)
		}

		// Create dummy polardbx backup
		objectFactory := factory.NewObjectFactory(rc)
		polardbxBackup, err := objectFactory.NewDummyPolarDBXBackup(metadata)
		if err != nil {
			return flow.Error(err, "Failed to new dummy polardbx backup")
		}
		err = rc.SetControllerRefAndCreate(polardbxBackup)
		if err != nil {
			return flow.Error(err, "Failed to create dummy polardbx backup")
		}
		polardbxSecretBackup, err := objectFactory.NewDummySecretBackup(metadata.PolarDBXClusterMetadata.Name, metadata)
		if err != nil {
			return flow.Error(err, "Failed to new dummy polardbx secret backup")
		}
		err = rc.SetControllerToOwnerAndCreate(polardbxBackup, polardbxSecretBackup)
		if err != nil {
			return flow.Error(err, "Failed to create dummy polardbx secret backup")
		}

		// Create dummy xstore backup and update its status
		for _, xstoreName := range metadata.GetXstoreNameList() {
			xstoreBackup, err := objectFactory.NewDummyXstoreBackup(xstoreName, polardbxBackup, metadata)
			if err != nil {
				return flow.Error(err, "Failed to new dummy xstore backup", "xstore", xstoreName)
			}
			err = rc.SetControllerToOwnerAndCreate(polardbxBackup, xstoreBackup)
			if err != nil {
				return flow.Error(err, "Failed to create dummy xstore backup", "xstore", xstoreName)
			}
			err = rc.Client().Status().Update(rc.Context(), xstoreBackup)
			if err != nil {
				return flow.Error(err, "Failed to update dummy xstore backup status", "xstore", xstoreName)
			}

			xstoreSecretBackup, err := objectFactory.NewDummySecretBackup(xstoreName, metadata)
			if err != nil {
				return flow.Error(err, "Failed to new dummy xstore secret backup", "xstore", xstoreName)
			}
			err = rc.SetControllerToOwnerAndCreate(polardbxBackup, xstoreSecretBackup)
			if err != nil {
				return flow.Error(err, "Failed to create dummy xstore secret backup", "xstore", xstoreName)
			}

			// record xstore and its backup for restore
			polardbxBackup.Status.Backups[xstoreName] = xstoreBackup.Name
		}

		// Update status of polardbx backup
		err = rc.Client().Status().Update(rc.Context(), polardbxBackup)
		if err != nil {
			return flow.Error(err, "Failed to update dummy polardbx backup status")
		}

		// The dummy backup object will be used in the later restore by setting it as backup set
		polardbx.Spec.Restore.BackupSet = polardbxBackup.Name
		err = rc.Client().Update(rc.Context(), polardbx)
		if err != nil {
			return flow.Error(err, "Failed to update backup set of restore spec")
		}

		return flow.Continue("Dummy backup object created!")
	})

// SyncSpecFromBackupSet aims to sync spec with original pxc cluster from backup set,
// if `SyncSpecWithOriginalCluster` is true, all the spec from original pxc will be applied to new pxc,
// otherwise only original dn replicas will be applied, currently restore does not support change DN replicas.
var SyncSpecFromBackupSet = polardbxv1reconcile.NewStepBinder("SyncSpecFromBackupSet",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		pxcBackup, err := rc.GetPXCBackupByName(polardbx.Spec.Restore.BackupSet)

		// just let the creation fail if pxb not found
		if err != nil || pxcBackup == nil {
			helper.TransferPhase(polardbx, polardbxv1polardbx.PhaseFailed)
			return flow.Error(errors.New("sync spec failed"), "Unable to get polardbx backup in current namespace",
				"pxb", polardbx.Spec.Restore.BackupSet, "error", err)
		}

		if polardbx.Spec.Restore.SyncSpecWithOriginalCluster {
			restoreSpec := polardbx.Spec.Restore.DeepCopy()
			serviceName := polardbx.Spec.ServiceName
			polardbx.Spec = *pxcBackup.Status.ClusterSpecSnapshot
			// ensure the operator can enter the restoring phase
			polardbx.Spec.Restore = restoreSpec
			// avoid using service name of original cluster
			polardbx.Spec.ServiceName = serviceName
		} else {
			// ensure that restored cluster have the same dn replicas with original cluster
			polardbx.Spec.Topology.Nodes.DN.Replicas = pxcBackup.Status.ClusterSpecSnapshot.Topology.Nodes.DN.Replicas
		}

		err = rc.Client().Update(rc.Context(), polardbx)
		if err != nil {
			return flow.Error(err, "Failed to sync topology from backup set")
		}
		return flow.Continue("Spec synced!")
	},
)

var CleanDummyBackupObject = polardbxv1reconcile.NewStepBinder("CleanDummyBackupObject",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		pxcBackup, err := rc.GetPXCBackupByName(polardbx.Spec.Restore.BackupSet)
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Failed to get polardbx backup", "backup set", pxcBackup.Name)
		}
		if pxcBackup == nil || pxcBackup.Annotations[polardbxmeta.AnnotationDummyBackup] != "true" {
			return flow.Continue("Dummy backup object not exists, just skip")
		}
		if err := rc.Client().Delete(rc.Context(), pxcBackup); err != nil {
			return flow.Error(err, "Failed to delete dummy backup object")
		}
		return flow.Continue("Dummy backup object cleaned!")
	},
)
