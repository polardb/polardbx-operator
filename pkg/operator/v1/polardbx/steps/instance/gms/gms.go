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

package gms

import (
	"errors"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/util/network"
	corev1 "k8s.io/api/core/v1"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxreconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	dictutil "github.com/alibaba/polardbx-operator/pkg/util/dict"
)

var InitializeSchemas = polardbxreconcile.NewStepBinder("InitializeSchemas",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}

		existed, err := mgr.IsMetaDBExisted()
		if err != nil {
			return flow.Error(err, "Unable to determine the status of GMS schemas.")
		}

		if !existed {
			if polardbx.Spec.Readonly {
				return flow.RetryAfter(10*time.Second, "Wait for master cluster to create metadb schema")
			}
			err = mgr.InitializeMetaDBSchema()
			if err != nil {
				return flow.Error(err, "Unable to initialize GMS schemas.")
			}
		}

		initialized, err := mgr.IsMetaDBInitialized(polardbx.Name)
		if err != nil {
			return flow.Error(err, "Unable to determine the status of GMS schemas.")
		}

		if !initialized {
			err = mgr.InitializeMetaDBInfo(polardbx.Spec.Readonly)
			if err != nil {
				return flow.Error(err, "Unable to initialize GMS info.")
			}
		}

		return flow.Continue("GMS schemas initialized.")
	},
)

func getHashFromXstoreName(xstoreNames interface{}) (string, error) {
	switch xstoreNames.(type) {
	case []string:
		for _, xstoreName := range xstoreNames.([]string) {
			splitXStoreName := strings.Split(xstoreName, "-")
			if splitXStoreName[len(splitXStoreName)-2] == "dn" { // hack way to get pxc hash
				return splitXStoreName[len(splitXStoreName)-3], nil
			}
		}
	case map[string]string:
		for xstoreName := range xstoreNames.(map[string]string) {
			splitXStoreName := strings.Split(xstoreName, "-")
			if splitXStoreName[len(splitXStoreName)-2] == "dn" { // hack way to get pxc hash
				return splitXStoreName[len(splitXStoreName)-3], nil
			}
		}
	}
	return "", errors.New("failed to get hash from name of xstore")
}

// getOriginalPxcInfo is a helper function to extract hash and name of original pxc during restore
func getOriginalPxcInfo(rc *polardbxreconcile.Context) (string, string, error) {
	polardbx := rc.MustGetPolarDBX()
	backup := &polardbxv1.PolarDBXBackup{}
	var err error
	if polardbx.Spec.Restore.BackupSet == "" && len(polardbx.Spec.Restore.BackupSet) == 0 {
		backup, err = rc.GetCompletedPXCBackup(map[string]string{polardbxmeta.LabelName: polardbx.Spec.Restore.From.PolarBDXName})
	} else {
		backup, err = rc.GetPXCBackupByName(polardbx.Spec.Restore.BackupSet)
	}
	if err != nil {
		return "", "", err
	}

	var pxcHash, pxcName string
	if backup != nil {
		pxcName = backup.Spec.Cluster.Name
		for _, xstoreName := range backup.Status.XStores {
			splitXStoreName := strings.Split(xstoreName, "-")
			if splitXStoreName[len(splitXStoreName)-2] == "dn" { // hack way to get pxc hash
				pxcHash = splitXStoreName[len(splitXStoreName)-3]
			}
		}
		return pxcHash, pxcName, nil
	}
	return "", "", errors.New("failed to get hash from name of xstore")
}

var RestoreSchemas = polardbxreconcile.NewStepBinder("RestoreSchemas",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polarDBX := rc.MustGetPolarDBX()
		originalPXCHash, originalPXCName, err := getOriginalPxcInfo(rc)
		if err != nil {
			return flow.Error(err, "Get oldXStoreName Failed")
		}
		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}

		restored, err := mgr.IsGmsSchemaRestored()
		if err != nil {
			return flow.Error(err, "Unable to determine the status of GMS schemas.")
		}

		if !restored {
			err = mgr.RestoreSchemas(originalPXCName, originalPXCHash, polarDBX.Status.Rand)
			if err != nil {
				return flow.Error(err, "Unable to restore GMS schemas.")
			}
			flow.Logger().Info("restore GMS schemas success")
		}

		return flow.Continue("GMS schemas restored.")
	},
)

var CreateAccounts = polardbxreconcile.NewStepBinder("CreateAccounts",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}

		accountSecret, err := rc.GetPolarDBXSecret(convention.SecretTypeAccount)
		if err != nil {
			return flow.Error(err, "Unable to get account secret.")
		}

		privileges := polardbx.Spec.Privileges
		rootAccountDefined := false
		for _, priv := range privileges {
			if priv.Username == convention.RootAccount {
				rootAccountDefined = true
				priv.Type = polardbxv1polardbx.Super
			}
			passwd, ok := accountSecret.Data[priv.Username]
			if !ok {
				flow.Logger().Info("Ignore account " + priv.Username)
				continue
			}

			privType := gms.GrantCustomPrivilege
			switch priv.Type {
			case polardbxv1polardbx.Super:
				privType = gms.GrantSuperPrivilege
			case polardbxv1polardbx.ReadWrite:
				privType = gms.GrantReadWritePrivilege
			case polardbxv1polardbx.ReadOnly:
				privType = gms.GrantReadOnlyPrivilege
			case polardbxv1polardbx.DDLOnly:
				privType = gms.GrantDdlPrivilege
			case polardbxv1polardbx.DMLOnly:
				privType = gms.GrantReadWritePrivilege
			}

			err := mgr.CreateDBAccount(priv.Username, string(passwd), &gms.GrantOption{
				Type: privType,
			})
			if err != nil {
				return flow.Error(err, "Unable to create account.", "username", priv.Username)
			}
		}

		if !rootAccountDefined {
			err := mgr.CreateDBAccount(convention.RootAccount,
				string(accountSecret.Data[convention.RootAccount]),
				&gms.GrantOption{Type: gms.GrantSuperPrivilege})
			if err != nil {
				return flow.Error(err, "Unable to create account.", "username", convention.RootAccount)
			}
		}

		return flow.Continue("Accounts created.")
	},
)

func SyncDynamicConfigs(force bool) control.BindFunc {
	stepName := "SyncDynamicConfigs"
	if force {
		stepName = "ForceSyncDynamicConfigs"
	}
	return polardbxreconcile.NewStepBinder(stepName, func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		if !force && polardbx.Status.ObservedGeneration == polardbx.Generation {
			return flow.Pass()
		}

		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}

		observedConfigs, err := mgr.ListDynamicParams()
		if err != nil {
			return flow.Error(err, "Unable to list current configs.")
		}

		// Dynamic part always uses the spec.
		configs := polardbx.Spec.Config.CN.Dynamic
		targetDynamicConfigs := gms.ConvertIntOrStringMapToStringMap(configs)

		// Initialize template parameters
		if polardbx.Spec.ParameterTemplate.Name != "" {
			parameter := rc.MustGetPolarDBXParameterTemplate(polardbx.Spec.ParameterTemplate.Name)
			params := parameter.Spec.NodeType.CN.ParamList
			for _, param := range params {
				// dynamic config priority higher than parameter template
				_, exist := targetDynamicConfigs[param.Name]
				if !exist {
					targetDynamicConfigs[param.Name] = param.DefaultValue
				}
			}
		}

		toUpdateDynamicConfigs := dictutil.DiffStringMap(targetDynamicConfigs, observedConfigs)
		if len(toUpdateDynamicConfigs) > 0 {
			flow.Logger().Info("Syncing dynamic configs...")
			err := mgr.SyncDynamicParams(toUpdateDynamicConfigs)
			if err != nil {
				return flow.Error(err, "Unable to sync dynamic configs.")
			}
			return flow.Continue("Dynamic configs synced.")
		}
		return flow.Pass()
	})
}

// For trailing stores which should be deleted,
// we only care about their id, rather than other infos
func transformIntoTrailingStorageInfosWithOnlyId(xstores []*polardbxv1.XStore) []gms.StorageNodeInfo {
	storageInfos := make([]gms.StorageNodeInfo, 0, len(xstores))
	for _, xstore := range xstores {
		storageInfos = append(storageInfos, gms.StorageNodeInfo{
			Id: xstore.Name,
		})
	}
	return storageInfos
}

func transformIntoStorageInfos(rc *polardbxreconcile.Context, polardbx *polardbxv1.PolarDBXCluster, xstores []*polardbxv1.XStore) ([]gms.StorageNodeInfo, error) {
	topology := polardbx.Status.SpecSnapshot.Topology
	cpuLimit := topology.Nodes.DN.Template.Resources.Limits.Cpu().Value()
	memSize := topology.Nodes.DN.Template.Resources.Limits.Memory().Value()

	storageInfos := make([]gms.StorageNodeInfo, 0, len(xstores))

	for _, xstore := range xstores {
		readonly := xstore.Spec.Readonly
		var service *corev1.Service
		var err error

		if readonly {
			service, err = rc.GetService(xstoreconvention.NewServiceName(xstore, xstoreconvention.ServiceTypeReadOnly))
			if err != nil {
				return nil, fmt.Errorf("unable to get readonly service of xstore " + xstore.Name)
			}
		} else {
			service, err = rc.GetService(xstoreconvention.NewServiceName(xstore, xstoreconvention.ServiceTypeReadWrite))
			if err != nil {
				return nil, fmt.Errorf("unable to get read-write service of xstore " + xstore.Name)
			}
		}

		secretName := xstoreconvention.NewSecretName(xstore)

		if xstore.Spec.Readonly {
			secretName = xstore.Spec.PrimaryXStore
		}

		accountSecret, err := rc.GetSecret(secretName)
		if err != nil {
			return nil, fmt.Errorf("unable to get account secret of xstore " + xstore.Name)
		}

		accessPort := k8shelper.MustGetPortFromService(service, xstoreconvention.PortAccess).Port
		xProtocolPort, privateServicePort := int32(-1), k8shelper.GetPortFromService(service, "polarx")
		if privateServicePort != nil {
			xProtocolPort = privateServicePort.Port
		}

		annoStorageType, _ := polardbx.Annotations[polardbxmeta.AnnotationStorageType]
		storageType, err := gms.GetStorageType(xstore.Spec.Engine, xstore.Status.EngineVersion, annoStorageType)
		if err != nil {
			return nil, err
		}

		masterInstId, storageKind := xstore.Name, gms.StorageKindMaster

		if readonly {
			masterInstId, storageKind = xstore.Spec.PrimaryXStore, gms.StorageKindSlave
		}

		storageInfos = append(storageInfos, gms.StorageNodeInfo{
			Id:            xstore.Name,
			MasterId:      masterInstId,
			ClusterId:     polardbx.Name,
			Host:          k8shelper.GetServiceDNSRecordWithSvc(service, true),
			Port:          accessPort,
			XProtocolPort: xProtocolPort,
			User:          xstoreconvention.SuperAccount,
			Passwd:        string(accountSecret.Data[xstoreconvention.SuperAccount]),
			Type:          storageType,
			Kind:          storageKind,
			MaxConn:       (1 << 16) - 1,
			CpuCore:       int32(cpuLimit),
			MemSize:       memSize,
			IsVip:         gms.IsVip,
		})

		// Add readonly pod cluster ip service, since CN will not fetch it
		if readonly {
			nodeSets := xstore.Spec.Topology.NodeSets
			for index, nodeSet := range nodeSets {
				podName := xstoreconvention.NewPodName(xstore, &nodeSet, index)
				containerPort := int32(xstore.Status.PodPorts[podName].ToMap()[convention.PortAccess])
				xPort := network.GetXPort(containerPort)
				storageInfos = append(storageInfos, gms.StorageNodeInfo{
					Id:            xstore.Name,
					MasterId:      masterInstId,
					ClusterId:     polardbx.Name,
					Host:          xstoreconvention.NewClusterIpServiceName(podName),
					Port:          containerPort,
					XProtocolPort: xPort,
					User:          xstoreconvention.SuperAccount,
					Passwd:        string(accountSecret.Data[xstoreconvention.SuperAccount]),
					Type:          storageType,
					Kind:          storageKind,
					MaxConn:       (1 << 16) - 1,
					CpuCore:       int32(cpuLimit),
					MemSize:       memSize,
					IsVip:         gms.IsNotVip,
				})
			}
		}
	}

	return storageInfos, nil
}

var EnableDNs = polardbxreconcile.NewStepBinder("EnableDNs",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		topology := polardbx.Status.SpecSnapshot.Topology
		replicas := topology.Nodes.DN.Replicas

		dnStores, err := rc.GetOrderedDNList()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}

		storageInfos, err := transformIntoStorageInfos(rc, polardbx, dnStores[:replicas])
		if err != nil {
			return flow.Error(err, "Unable to transform xstores into storage infos.")
		}

		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}

		err = mgr.EnableStorageNodes(storageInfos...)
		if err != nil {
			return flow.Error(err, "Unable to enable storage nodes in GMS.")
		}
		return flow.Continue("DNs are enabled!")
	},
)

var DisableTrailingDNs = polardbxreconcile.NewStepBinder("DisableTrailingDNs",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		topology := polardbx.Status.SpecSnapshot.Topology
		replicas := topology.Nodes.DN.Replicas

		dnStores, err := rc.GetOrderedDNList()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}
		trailingStores := make([]*polardbxv1.XStore, 0)
		for i, xstore := range dnStores {
			if i >= int(replicas) {
				if controllerutil.ContainsFinalizer(xstore, polardbxmeta.Finalizer) {
					trailingStores = append(trailingStores, xstore)
				}
			}
		}

		if len(trailingStores) == 0 {
			return flow.Pass()
		}

		storageInfoIds := transformIntoTrailingStorageInfosWithOnlyId(trailingStores)

		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}
		err = mgr.DisableStorageNodes(storageInfoIds...)
		if err != nil {
			return flow.Error(err, "Unable to enable trailing storage nodes in GMS.")
		}

		// Remove finalizers on trailing stores.
		for _, xstore := range trailingStores {
			controllerutil.RemoveFinalizer(xstore, polardbxmeta.Finalizer)
			err := rc.Client().Update(rc.Context(), xstore)
			if err != nil {
				return flow.Error(err, "Unable to remove finalizer from xstore.", "xstore", xstore.Name)
			}
		}

		return flow.Continue("Trailing DNs are disabled!")
	},
)

var LockReadWrite = polardbxreconcile.NewStepBinder("LockReadWrite",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}
		err = mgr.Lock()
		if err != nil {
			return flow.Error(err, "Unable to lock.")
		}
		return flow.Continue("Locked.")
	},
)

var UnlockReadWrite = polardbxreconcile.NewStepBinder("UnlockReadWrite",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}
		err = mgr.Unlock()
		if err != nil {
			return flow.Error(err, "Unable to unlock.")
		}
		return flow.Continue("Unlocked.")
	},
)
