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
	"fmt"

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
		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}

		initialized, err := mgr.IsMetaDBInitialized()
		if err != nil {
			return flow.Error(err, "Unable to determine the status of GMS schemas.")
		}

		if !initialized {
			err = mgr.InitializeMetaDB()
			if err != nil {
				return flow.Error(err, "Unable to initialize GMS schemas.")
			}
		}

		return flow.Continue("GMS schemas initialized.")
	},
)

var RestoreSchemas = polardbxreconcile.NewStepBinder("RestoreSchemas",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}

		restored, err := mgr.IsGmsSchemaRestored()
		if err != nil {
			return flow.Error(err, "Unable to determine the status of GMS schemas.")
		}

		if !restored {
			err = mgr.RestoreSchemas( /*TODO*/ "fake")
			if err != nil {
				return flow.Error(err, "Unable to restore GMS schemas.")
			}
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

func transformIntoStorageInfos(rc *polardbxreconcile.Context, xstores []*polardbxv1.XStore) ([]gms.StorageNodeInfo, error) {
	polardbx := rc.MustGetPolarDBX()
	topology := polardbx.Status.SpecSnapshot.Topology
	cpuLimit := topology.Nodes.DN.Template.Resources.Limits.Cpu().Value()
	memSize := topology.Nodes.DN.Template.Resources.Limits.Memory().Value()

	storageInfos := make([]gms.StorageNodeInfo, 0, len(xstores))

	for _, xstore := range xstores {
		rwService, err := rc.GetService(xstoreconvention.NewServiceName(xstore, xstoreconvention.ServiceTypeReadWrite))
		if err != nil {
			return nil, fmt.Errorf("unable to get read-write service of xstore " + xstore.Name)
		}

		accountSecret, err := rc.GetSecret(xstoreconvention.NewSecretName(xstore))
		if err != nil {
			return nil, fmt.Errorf("unable to get account secret of xstore " + xstore.Name)
		}

		accessPort := k8shelper.MustGetPortFromService(rwService, xstoreconvention.PortAccess).Port
		xProtocolPort, privateServicePort := int32(-1), k8shelper.GetPortFromService(rwService, "polarx")
		if privateServicePort != nil {
			xProtocolPort = privateServicePort.Port
		}

		storageType, err := gms.GetStorageType(xstore.Spec.Engine, xstore.Status.EngineVersion)
		if err != nil {
			return nil, err
		}

		storageInfos = append(storageInfos, gms.StorageNodeInfo{
			Id:            xstore.Name,
			Host:          k8shelper.GetServiceDNSRecordWithSvc(rwService, true),
			Port:          accessPort,
			XProtocolPort: xProtocolPort,
			User:          xstoreconvention.SuperAccount,
			Passwd:        string(accountSecret.Data[xstoreconvention.SuperAccount]),
			Type:          storageType,
			Kind:          gms.StorageKindMaster,
			MaxConn:       (1 << 16) - 1,
			CpuCore:       int32(cpuLimit),
			MemSize:       memSize,
		})
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

		storageInfos, err := transformIntoStorageInfos(rc, dnStores[:replicas])
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

		storageInfos, err := transformIntoStorageInfos(rc, trailingStores)
		if err != nil {
			return flow.Error(err, "Unable to transform xstores into storage infos.")
		}

		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}
		err = mgr.DisableStorageNodes(storageInfos...)
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
