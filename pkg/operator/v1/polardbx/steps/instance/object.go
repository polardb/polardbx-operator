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

package instance

import (
	"fmt"
	"k8s.io/apimachinery/pkg/api/equality"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/factory"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
)

var CreateSecretsIfNotFound = polardbxv1reconcile.NewStepBinder("CreateSecretsIfNotFound",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polarDBX := rc.MustGetPolarDBX()
		accountSecret, err := rc.GetPolarDBXSecret(convention.SecretTypeAccount)
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get account secret.")
		}
		if accountSecret == nil {
			if polarDBX.Spec.Restore != nil {
				if client.IgnoreNotFound(err) != nil {
					return flow.Error(err, "Unable to get original secret for restore")
				}
				accountSecret, err = factory.NewObjectFactory(rc).NewSecretForRestore()
				if err != nil {
					return flow.Error(err, "Unable to new account secret during restoring.")
				}
			}
			if accountSecret == nil || len(accountSecret.Data) == 0 {
				accountSecret, err = factory.NewObjectFactory(rc).NewSecret()
				if err != nil {
					return flow.Error(err, "Unable to new account secret.")
				}
			}
			err = rc.SetControllerRefAndCreate(accountSecret)
			if err != nil {
				return flow.Error(err, "Unable to create account secret.")
			}
		}

		keySecret, err := rc.GetPolarDBXSecret(convention.SecretTypeSecurity)
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get encode key secret.")
		}
		if keySecret == nil {
			keySecret, err = factory.NewObjectFactory(rc).NewSecuritySecret()
			if err != nil {
				return flow.Error(err, "Unable to new encode key secret.")
			}
			err = rc.SetControllerRefAndCreate(keySecret)
			if err != nil {
				return flow.Error(err, "Unable to create encode key secret.")
			}
		}
		return flow.Pass()
	},
)

var CreateServicesIfNotFound = polardbxv1reconcile.NewStepBinder("CreateServicesIfNotFound",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		objectFactory := factory.NewObjectFactory(rc)

		polardbx := rc.MustGetPolarDBX()

		serviceTypes := []convention.ServiceType{
			convention.ServiceTypeReadOnly,
		}

		if !polardbx.Spec.Readonly {
			serviceTypes = []convention.ServiceType{
				convention.ServiceTypeCDCMetrics,
				convention.ServiceTypeReadWrite,
			}
		}

		for _, serviceType := range serviceTypes {
			service, err := rc.GetPolarDBXService(serviceType)
			if client.IgnoreNotFound(err) != nil {
				return flow.Error(err, "Unable to get service", "service-type", serviceType)
			}

			if service == nil {
				switch serviceType {
				case convention.ServiceTypeReadWrite:
					service, err = objectFactory.NewService()
				case convention.ServiceTypeReadOnly:
					service, err = objectFactory.NewReadOnlyService()
				case convention.ServiceTypeCDCMetrics:
					service, err = objectFactory.NewCDCMetricsService()
				default:
					panic("unimplemented")
				}

				if err != nil {
					return flow.Error(err, "Unable new service.", "service-type", serviceType)
				}

				err = rc.SetControllerRefAndCreate(service)
				if err != nil {
					return flow.Error(err, "Unable to create service.", "service-type", serviceType)
				}
			}
		}

		return flow.Pass()
	},
)

var CreateConfigMapsIfNotFound = polardbxv1reconcile.NewStepBinder("CreateConfigMapsIfNotFound",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		objectFactory := factory.NewObjectFactory(rc)

		for _, cmType := range []convention.ConfigMapType{
			convention.ConfigMapTypeConfig,
			convention.ConfigMapTypeTask,
		} {
			cm, err := rc.GetPolarDBXConfigMap(cmType)
			if client.IgnoreNotFound(err) != nil {
				return flow.Error(err, "Unable to get config map.", "configmap-type", cmType)
			}

			if cm == nil {
				cm, err := objectFactory.NewConfigMap(cmType)
				if err != nil {
					return flow.Error(err, "Unable to new config map.", "configmap-type", cmType)
				}

				err = rc.SetControllerRefAndCreate(cm)
				if err != nil {
					return flow.Error(err, "Unable to create config map.", "configmap-type", cmType)
				}
			}
		}

		return flow.Pass()
	},
)

var CreateOrReconcileGMS = polardbxv1reconcile.NewStepBinder("CreateOrReconcileGMS",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		if polardbx.Spec.Readonly {
			return flow.Continue("Readonly pxc, skip.")
		}

		if polardbx.Spec.ShareGMS {
			return flow.Continue("GMS shared, skip.")
		}

		gmsStore, err := rc.GetGMS()
		if client.IgnoreNotFound(err) != nil {
			return flow.Continue("Unable to get xstore of GMS.")
		}
		if gmsStore == nil {
			// Only valid when creating/restoring... Consider cluster is broken when
			// GMS not found in other phases, so transfer the phase into failed.
			if !helper.IsPhaseIn(polardbx, polardbxv1polardbx.PhaseCreating, polardbxv1polardbx.PhaseRestoring) {
				helper.TransferPhase(polardbx, polardbxv1polardbx.PhaseFailed)
				return flow.Retry("GMS not found, transfer into failed.")
			}

			objectFactory := factory.NewObjectFactory(rc)
			gmsStore, err := objectFactory.NewXStoreGMS()
			if err != nil {
				return flow.Error(err, "Unable to new xstore of GMS.")
			}

			err = rc.SetControllerRefAndCreate(gmsStore)
			if err != nil {
				return flow.Error(err, "Unable to create xstore of GMS.")
			}

			return flow.Continue("GMS xstore created!")
		} else {
			gmsStoreGeneration, err := convention.GetGenerationLabelValue(gmsStore)
			if err != nil {
				return flow.Error(err, "Unable to get generation from xstore of GMS.")
			}

			// Branch observed generation larger than generation on GMS xstore, update it!
			if gmsStoreGeneration < polardbx.Status.ObservedGeneration {
				// Skip upgrade if feature gate isn't enabled.
				if !featuregate.StoreUpgrade.Enabled() {
					return flow.Continue("Feature 'StoreUpgrade' not enabled, skip upgrade.", "xstore", gmsStore.Name)
				}

				objectFactory := factory.NewObjectFactory(rc)
				newGmsStore, err := objectFactory.NewXStoreGMS()
				if err != nil {
					return flow.Error(err, "Unable to new xstore of GMS.")
				}

				convention.CopyMetadataForUpdate(&newGmsStore.ObjectMeta, &gmsStore.ObjectMeta,
					polardbx.Status.ObservedGeneration)

				err = rc.Client().Update(rc.Context(), newGmsStore)
				if err != nil {
					return flow.Error(err, "Unable to update xstore of GMS.", "generation", polardbx.Status.ObservedGeneration)
				}

				return flow.Continue("GMS xstore updated!")
			}

			return flow.Pass()
		}
	},
)

var SyncDnReplicasAndCheckControllerRef = polardbxv1reconcile.NewStepBinder("SyncDnReplicasAndCheckControllerRef",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if !polardbx.Spec.Readonly {
			return flow.Pass()
		}

		primaryPolardbx, err := rc.GetPrimaryPolarDBX()
		if err != nil {
			return flow.RetryErr(err, "Failed to get primary pxc.")
		}

		if err = k8shelper.CheckControllerReference(polardbx, primaryPolardbx); err != nil {
			err = ctrl.SetControllerReference(primaryPolardbx, polardbx, rc.Scheme())
			if err != nil {
				return flow.Error(err, "Failed to set controller ref: %w")
			}
			rc.MarkPolarDBXChanged()
		}

		primaryDnReplicas := primaryPolardbx.Spec.Topology.Nodes.DN.Replicas

		if polardbx.Spec.Topology.Nodes.DN.Replicas != primaryDnReplicas {
			polardbx.Spec.Topology.Nodes.DN.Replicas = primaryDnReplicas
			rc.MarkPolarDBXChanged()
		}
		return flow.Pass()
	},
)

var CreateOrReconcileReadonlyPolardbx = polardbxv1reconcile.NewStepBinder("CreateOrReconcileReadonlyPolardbx",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if polardbx.Spec.Readonly {
			return flow.Pass()
		}

		objectFactory := factory.NewObjectFactory(rc)
		wg := &sync.WaitGroup{}
		readonlyPolardbxList := polardbx.Spec.InitReadonly
		errs := make([]error, len(readonlyPolardbxList))
		var errCnt uint32

		for i, readonlyInst := range readonlyPolardbxList {
			newReadonlyPolardbx, err := objectFactory.NewReadonlyPolardbx(readonlyInst)

			readonlyName := readonlyInst.Name

			if err != nil {
				return flow.Error(err, "Unable to new readonly pxc.", "name", readonlyName)
			}

			key := types.NamespacedName{
				Namespace: polardbx.Namespace,
				Name:      polardbx.Name + "-" + readonlyName,
			}

			if rc.CheckPolarDBXExist(key) {
				// Dismiss existed pxc
				continue
			} else {
				wg.Add(1)
				logger, idx := flow.Logger(), i
				go func() {
					defer wg.Done()
					err = rc.SetControllerRefAndCreate(newReadonlyPolardbx)
					if err != nil {
						logger.Error(err, "Unable to create readonly pxc.", "name", readonlyName)
						errs[idx] = err
						atomic.AddUint32(&errCnt, 1)
					}
				}()
			}
		}

		wg.Wait()
		if errCnt > 0 {
			var firstErr error
			for _, err := range errs {
				if err != nil {
					firstErr = err
					break
				}
			}
			return flow.Error(firstErr, "Unable to create or reconcile readonly pxc.", "error-cnt", errCnt)
		}

		return flow.Pass()
	},
)

var CreateOrReconcileDNs = polardbxv1reconcile.NewStepBinder("CreateOrReconcileDNs",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		readonly := polardbx.Spec.Readonly
		topology := polardbx.Status.SpecSnapshot.Topology
		observedGeneration := polardbx.Status.ObservedGeneration
		replicas := int(topology.Nodes.DN.Replicas)

		dnStores, err := rc.GetDNMap()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}

		// Ensure DNs are sorted and their indexes are incremental
		// when phase is not in creating or restoring.
		if !helper.IsPhaseIn(polardbx, polardbxv1polardbx.PhaseCreating, polardbxv1polardbx.PhaseRestoring) {
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
		}

		objectFactory := factory.NewObjectFactory(rc)
		anyChanged := false
		wg := &sync.WaitGroup{}
		errs := make([]error, replicas)
		var errCnt uint32
		for i := 0; i < replicas; i++ {
			observedDnStore, ok := dnStores[i]

			if !ok {
				newDnStore, err := objectFactory.NewXStoreDN(i)
				if err != nil {
					return flow.Error(err, "Unable to new xstore of DN.", "index", i)
				}

				wg.Add(1)
				logger, idx := flow.Logger(), i
				go func() {
					defer wg.Done()
					err = rc.SetControllerRefAndCreate(newDnStore)
					if err != nil {
						logger.Error(err, "Unable to create xstore of DN.", "index", i)
						errs[idx] = err
						atomic.AddUint32(&errCnt, 1)
					}
				}()

				anyChanged = true
			} else {
				generation, err := convention.GetGenerationLabelValue(observedDnStore)
				if err != nil {
					return flow.Error(err, "Unable to get generation from xstore of DN.", "index", i)
				}

				if generation < observedGeneration {
					// Skip upgrade if feature gate isn't enabled.
					if !featuregate.StoreUpgrade.Enabled() {
						flow.Logger().Info("Feature 'StoreUpgrade' not enabled, skip upgrade.", "xstore", observedDnStore.Name)
						continue
					}

					newDnStore, err := objectFactory.NewXStoreDN(i)
					if err != nil {
						return flow.Error(err, "Unable to new xstore of DN.", "index", i)
					}

					newDnStoreHash := newDnStore.Labels[xstoremeta.LabelHash]
					observedDnStoreHash := observedDnStore.Labels[xstoremeta.LabelHash]
					if newDnStoreHash != observedDnStoreHash {
						convention.CopyMetadataForUpdate(&newDnStore.ObjectMeta, &observedDnStore.ObjectMeta, observedGeneration)
						newDnStore.SetLabels(k8shelper.PatchLabels(newDnStore.Labels, map[string]string{
							xstoremeta.LabelHash: newDnStoreHash,
						}))
						wg.Add(1)
						logger, idx := flow.Logger(), i
						go func() {
							defer wg.Done()
							err = rc.Client().Update(rc.Context(), newDnStore)
							if err != nil {
								logger.Error(err, "Unable to update xstore of DN.", "index", i)
								errs[idx] = err
								atomic.AddUint32(&errCnt, 1)
							}
						}()
						anyChanged = true
					}

				}
			}
		}

		wg.Wait()
		if errCnt > 0 {
			var firstErr error
			for _, err := range errs {
				if err != nil {
					firstErr = err
					break
				}
			}
			return flow.Error(firstErr, "Unable to create or reconcile xstores of DN.", "error-cnt", errCnt)
		}

		// Remove DNs not enabled in GMS and larger than current target replicas (safe because not in use).
		toRemoveStores := make(map[int]*polardbxv1.XStore, 0)
		for index, xstore := range dnStores {
			if index >= replicas {
				toRemoveStores[index] = xstore
			}
		}
		if len(toRemoveStores) > 0 {
			mgr, err := rc.GetPolarDBXGMSManager()
			if err != nil {
				return flow.Error(err, "Unable to get manager for GMS.")
			}

			storageNodeIds := make(map[string]int)
			if initialized, err := mgr.IsMetaDBInitialized(polardbx.Name); err != nil {
				return flow.Error(err, "Unable to determine if GMS is initialized.")
			} else if initialized {
				storageKind := gms.StorageKindMaster
				if readonly {
					storageKind = gms.StorageKindSlave
				}
				storageNodes, err := mgr.ListStorageNodes(storageKind)
				if err != nil {
					return flow.Error(err, "Unable to list storage nodes in GMS.")
				}
				for _, s := range storageNodes {
					storageNodeIds[s.Id] = 1
				}
			} else {
				// No storage nodes.
			}

			canRemoveStoreIndices := make([]int, 0)
			for index, xstore := range toRemoveStores {
				if _, found := storageNodeIds[xstore.Name]; !found {
					canRemoveStoreIndices = append(canRemoveStoreIndices, index)
				}
			}
			if len(canRemoveStoreIndices) > 0 {
				sort.Slice(canRemoveStoreIndices, func(i, j int) bool {
					return canRemoveStoreIndices[i] > canRemoveStoreIndices[j]
				})
				flow.Logger().Info("Trying to remove trailing xstores (not in use)",
					"trailing-indices", canRemoveStoreIndices)
				for _, index := range canRemoveStoreIndices {
					xstore := dnStores[index]
					err := rc.Client().Delete(rc.Context(), xstore, client.PropagationPolicy(metav1.DeletePropagationBackground))
					if err != nil {
						return flow.Error(err, "Unable to delete unused trailing xstore.", "xstore", xstore.Name)
					}
				}
				flow.Logger().Info("Unused trailing xstores are all removed.")
			}
		}

		if anyChanged {
			return flow.Retry("DNs created or updated!")
		}

		return flow.Pass()
	},
)

var RemoveTrailingDNs = polardbxv1reconcile.NewStepBinder("RemoveTrailingDNs",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		topology := polardbx.Status.SpecSnapshot.Topology
		replicas := int(topology.Nodes.DN.Replicas)

		dnStores, err := rc.GetOrderedDNList()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}

		for i := len(dnStores) - 1; i >= replicas; i-- {
			if dnStores[i].DeletionTimestamp.IsZero() {
				err := rc.Client().Delete(rc.Context(), dnStores[i],
					client.PropagationPolicy(metav1.DeletePropagationBackground))
				if err != nil {
					return flow.Error(err, "Unable to remove trailing DN.", "xstore", dnStores[i].Name)
				}
			}
		}

		if len(dnStores) > replicas {
			return flow.Retry("Trailing DNs are deleted.")
		}
		return flow.Pass()
	},
)

func isDeployPodSpecChanged(oldDeploy *appsv1.Deployment, newDeploy *appsv1.Deployment) bool {
	// engine container
	oldPodSpec := oldDeploy.Spec.Template.Spec
	oldContainer := rebuildFromContainer(k8shelper.GetContainerFromPodSpec(&oldPodSpec, convention.ContainerEngine))
	newPodSpec := newDeploy.Spec.Template.Spec
	newContainer := rebuildFromContainer(k8shelper.GetContainerFromPodSpec(&newPodSpec, convention.ContainerEngine))
	return !equality.Semantic.DeepEqual(oldContainer, newContainer)
}

func rebuildFromContainer(container *corev1.Container) *corev1.Container {
	return &corev1.Container{
		Image:   container.Image,
		Ports:   container.Ports,
		EnvFrom: container.EnvFrom,
		Env:     container.Env,
	}
}

func reconcileGroupedDeployments(rc *polardbxv1reconcile.Context, flow control.Flow, role string) (reconcile.Result, error) {
	polardbxmeta.AssertRoleIn(role, polardbxmeta.RoleCN, polardbxmeta.RoleCDC, polardbxmeta.RoleColumnar)

	flow = flow.WithLoggerValues("role", role)

	polardbx := rc.MustGetPolarDBX()
	observedGeneration := polardbx.Status.ObservedGeneration

	observedDeployments, err := rc.GetDeploymentMap(role)
	if err != nil {
		return flow.Error(err, "Unable to get deployments.")
	}

	var deployments map[string]appsv1.Deployment
	if role == polardbxmeta.RoleCN {
		if !rc.HasCNs() {
			return flow.Pass()
		}
		deployments, err = factory.NewObjectFactory(rc).NewDeployments4CN()
	} else if role == polardbxmeta.RoleCDC {
		if polardbx.Spec.Readonly {
			return flow.Pass()
		}
		deployments, err = factory.NewObjectFactory(rc).NewDeployments4CDC()
	} else if role == polardbxmeta.RoleColumnar {
		deployments, err = factory.NewObjectFactory(rc).NewDeployments4Columnar()
	}
	if err != nil {
		return flow.Error(err, "Unable to new deployments.")
	}

	anyChanged := false

	// Update or delete outdated deployments
	for group, observedDeployment := range observedDeployments {
		generation, err := convention.GetGenerationLabelValue(observedDeployment)
		if err != nil {
			return flow.Error(err, "Unable to get generation from deployment.",
				"deployment", observedDeployment.Name)
		}

		if generation < observedGeneration {
			newDeployment, ok := deployments[group]
			if !ok {
				err := rc.Client().Delete(rc.Context(), observedDeployment,
					client.PropagationPolicy(metav1.DeletePropagationBackground))
				if err != nil {
					return flow.Error(err, "Unable to delete deployment.",
						"deployment", observedDeployment.Name)
				}
				anyChanged = true
			} else {
				if *(newDeployment.Spec.Replicas) != *(observedDeployment.Spec.Replicas) {
					observedDeployment.Spec.Replicas = newDeployment.Spec.Replicas
					err := rc.Client().Update(rc.Context(), observedDeployment)
					if err != nil {
						return flow.Error(err, "Unable to update deployment.",
							"deployment", observedDeployment.Name)
					}

				} else if isDeployPodSpecChanged(observedDeployment, &newDeployment) {
					newDeployHash := newDeployment.Labels[polardbxmeta.LabelHash]
					convention.CopyMetadataForUpdate(&newDeployment.ObjectMeta, &observedDeployment.ObjectMeta, observedGeneration)
					newDeployment.SetLabels(k8shelper.PatchLabels(newDeployment.Labels, map[string]string{
						polardbxmeta.LabelHash: newDeployHash,
					}))
					err := rc.Client().Update(rc.Context(), &newDeployment)
					if err != nil {
						return flow.Error(err, "Unable to update deployment.",
							"deployment", observedDeployment.Name)
					}
					anyChanged = true
				}
			}

		}
	}

	// Create new deployments if not found
	for group, deployment := range deployments {
		if _, ok := observedDeployments[group]; ok {
			continue
		}

		err := rc.SetControllerRefAndCreate(&deployment)
		if err != nil {
			return flow.Error(err, "Unable to create deployment.", "deployment", deployment.Name)
		}

		anyChanged = true
	}

	if anyChanged {
		return flow.Retry("Deployments reconciled.")
	}

	return flow.Pass()
}

var CreateFileStorage = polardbxv1reconcile.NewStepBinder("CreateFileStorage",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		groupManager, err := rc.GetPolarDBXGroupManager()
		if err != nil {
			return flow.Error(err, "Failed to get CN group manager.")
		}

		supportFileStorage, err := groupManager.CheckFileStorageCompatibility()
		if err != nil {
			return flow.Error(err, "Failed to check compatibility of file storage.")
		}
		if !supportFileStorage {
			return flow.Continue("Current pxc does not support file storage.")
		}

		fileStorageInfoList, err := groupManager.ListFileStorage()
		if err != nil {
			return flow.Error(err, "Failed to get file storage list")
		}

		config := rc.Config()

		for _, info := range polardbx.Spec.Config.CN.ColdDataFileStorage {
			//  check if the filestorage already existed
			if info.CheckEngineExists(fileStorageInfoList) {
				continue
			}

			// create the file storage
			err = groupManager.CreateFileStorage(info, config)
			if err != nil {
				return flow.Error(err, fmt.Sprintf("Failed to create file storage for ColdData: %s.", info.Engine))
			}
		}
		if polardbx.Status.SpecSnapshot.Topology.Nodes.Columnar != nil {
			for _, info := range polardbx.Spec.Config.Columnar.ColumnarDataFileStorage {
				// check if the filestorage already existed
				if info.CheckEngineExists(fileStorageInfoList) {
					continue
				}
				err = groupManager.CreateFileStorage(info, config)
				if err != nil {
					flow.Logger().Error(err, fmt.Sprintf("Failed to create file storage for Columnar: %s.", info.Engine))
				}
			}
		}
		return flow.Pass()
	},
)

var ModifyDBAAccountTypeIfNeeded = polardbxv1reconcile.NewStepBinder("ModifyDBAAccountTypeIfNeeded",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		mgr, err := rc.GetPolarDBXGMSManager()
		if err != nil {
			return flow.Error(err, "Unable to get GMS manager.")
		}

		privileges := polardbx.Spec.Privileges
		for _, priv := range privileges {
			if priv.Type != polardbxv1polardbx.Super {
				continue
			}

			// Account: polardbx_root does not modify account type
			if priv.Username == convention.RootAccount {
				continue
			}

			// Only super account need to set to DBA
			err = mgr.ModifyDBAccountType(priv.Username, gms.AccountTypeDBA)
			if err != nil {
				return flow.Error(err, "Unable to modify account type.", "username", convention.RootAccount)
			}
		}

		return flow.Pass()
	},
)

var CreateOrReconcileCNs = polardbxv1reconcile.NewStepBinder("CreateOrReconcileCNs",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		return reconcileGroupedDeployments(rc, flow, polardbxmeta.RoleCN)
	},
)

var CreateOrReconcileCDCs = polardbxv1reconcile.NewStepBinder("CreateOrReconcileCDCs",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		return reconcileGroupedDeployments(rc, flow, polardbxmeta.RoleCDC)
	},
)

var CreateOrReconcileColumnars = polardbxv1reconcile.NewStepBinder("CreateOrReconcileColumnars",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		return reconcileGroupedDeployments(rc, flow, polardbxmeta.RoleColumnar)
	},
)

var SyncReadonlyDnStorageInfo = polardbxv1reconcile.NewStepBinder("SyncReadonlyDnStorageInfo",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if !polardbx.Spec.Readonly {
			return flow.Pass()
		}
		// check lastest create time of dn readonly pod
		dnPods, err := rc.GetPods(polardbxmeta.RoleDN)
		if err != nil {
			return flow.RetryErr(err, "failed to get dn pods")
		}
		expectedStorageNodes := make([]gms.StorageNodeInfo, 0, len(dnPods))
		for _, pod := range dnPods {
			if pod.DeletionTimestamp.IsZero() && pod.Status.PodIP != "" {
				accessPort := k8shelper.MustGetPortFromContainer(
					k8shelper.MustGetContainerFromPod(&pod, convention.ContainerEngine),
					convention.PortAccess,
				).ContainerPort
				polarxPort := k8shelper.MustGetPortFromContainer(
					k8shelper.MustGetContainerFromPod(&pod, convention.ContainerEngine),
					"polarx",
				).ContainerPort
				expectedStorageNodes = append(expectedStorageNodes, gms.StorageNodeInfo{
					ClusterId:     pod.Labels[polardbxmeta.LabelName],
					Id:            pod.Labels[xstoremeta.LabelName],
					IsVip:         gms.IsNotVip,
					Host:          pod.Status.PodIP,
					Port:          accessPort,
					XProtocolPort: polarxPort,
				})
			}
		}
		hash, err := security.HashObj(expectedStorageNodes)
		if err != nil {
			return flow.RetryErr(err, "failed to hash expectedStorageNodes")
		}
		if polardbx.Status.ReadonlyStorageInfoHash != hash {
			gmsManager, err := rc.GetPolarDBXGMSManager()
			if err != nil {
				return flow.RetryErr(err, "failed to get gms manager")
			}
			err = gmsManager.UpdateRoStorageNodes(expectedStorageNodes...)
			if err != nil {
				return flow.RetryErr(err, "failed to update ro storage nodes")
			}
			polardbx.Status.ReadonlyStorageInfoHash = hash
		}
		return flow.Continue("SyncReadonlyDnStorageInfo success")
	},
)

func isXStoreReady(xstore *polardbxv1.XStore) bool {
	return xstore.Status.ObservedGeneration == xstore.Generation &&
		xstore.Status.Phase == polardbxv1xstore.PhaseRunning
}

var WaitUntilCNCDCPodsReady = polardbxv1reconcile.NewStepBinder("WaitUntilCNCDCPodsReady",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		cnPods, err := rc.GetPods(polardbxmeta.RoleCN)
		if err != nil {
			return flow.Error(err, "Unable to get pods for CN")
		}

		unready := k8shelper.FilterPodsBy(cnPods, func(pod *corev1.Pod) bool {
			return !k8shelper.IsPodReady(pod)
		})

		if len(unready) > 0 {
			return flow.Wait("Found unready cn pods, keep waiting...", "unready-pods",
				strings.Join(k8shelper.ToObjectNames(unready), ","))
		}

		cdcPods, err := rc.GetPods(polardbxmeta.RoleCDC)
		if err != nil {
			return flow.Error(err, "Unable to get pods for CDC")
		}

		unready = k8shelper.FilterPodsBy(cdcPods, func(pod *corev1.Pod) bool {
			return !k8shelper.IsPodReady(pod)
		})

		if len(unready) > 0 {
			return flow.Wait("Found unready cdc pods, keep waiting...", "unready-pods",
				strings.Join(k8shelper.ToObjectNames(unready), ","))
		}

		return flow.Pass()
	},
)

var WaitUntilGMSReady = polardbxv1reconcile.NewStepBinder("WaitUntilGMSReady",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		gms, err := rc.GetGMS()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get xstore of GMS.")
		}
		if gms == nil {
			return flow.Wait("XStore of GMS not created.")
		}

		if gms.Status.Phase == polardbxv1xstore.PhaseFailed {
			helper.TransferPhase(rc.MustGetPolarDBX(), polardbxv1polardbx.PhaseFailed)
			return flow.Retry("XStore of GMS is failed, transfer phase into failed.")
		}

		if isXStoreReady(gms) {
			return flow.Continue("XStore of GMS is ready.")
		} else {
			return flow.Wait("XStore of GMS isn't ready, wait.", "xstore", gms.Name, "xstore.phase", gms.Status.Phase)
		}
	},
)

var WaitUntilDNsReady = polardbxv1reconcile.NewStepBinder("WaitUntilDNsReady",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		topology := &polardbx.Status.SpecSnapshot.Topology

		dnStores, err := rc.GetDNMap()
		if err != nil {
			return flow.Error(err, "Unable to list xstores of DNs.")
		}

		notReadyCnt, skipCnt := 0, 0
		for i, dnStore := range dnStores {
			// Trailing DNs.
			if i >= int(topology.Nodes.DN.Replicas) {
				skipCnt++
				continue
			}

			if dnStore.Status.Phase == polardbxv1xstore.PhaseFailed {
				helper.TransferPhase(rc.MustGetPolarDBX(), polardbxv1polardbx.PhaseFailed)
				return flow.Retry("XStore of DN is failed, transfer phase into failed.", "xstore", dnStore.Name)
			}

			if !isXStoreReady(dnStore) {
				notReadyCnt++
			}
		}

		if notReadyCnt > 0 {
			return flow.Wait("Some xstore of DN is not ready, wait.", "not-ready", notReadyCnt, "skip", skipCnt)
		}

		return flow.Continue("XStores of DNs are ready.", "skip", skipCnt)
	},
)

func areDeploymentsRolledOut(deployments map[string]*appsv1.Deployment) bool {
	for _, deploy := range deployments {
		if !k8shelper.IsDeploymentRolledOut(deploy) {
			return false
		}
	}
	return true
}

var WaitUntilCNDeploymentsRolledOut = polardbxv1reconcile.NewStepBinder("WaitUntilCNDeploymentsRolledOut",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		cnDeployments, err := rc.GetDeploymentMap(polardbxmeta.RoleCN)
		if err != nil {
			return flow.Error(err, "Unable to get deployments of CN.")
		}

		if areDeploymentsRolledOut(cnDeployments) {
			return flow.Continue("Deployments of CN are rolled out.")
		}
		return flow.Wait("Some deployment of CN is rolling.")
	},
)

var WaitUntilPrimaryCNDeploymentsRolledOut = polardbxv1reconcile.NewStepBinder("WaitUntilPrimaryCNDeploymentsRolledOut",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		cnDeployments, err := rc.GetPrimaryDeploymentMap(polardbxmeta.RoleCN)
		if err != nil {
			return flow.Error(err, "Unable to get deployments of primary CN.")
		}

		if len(cnDeployments) > 0 && areDeploymentsRolledOut(cnDeployments) {
			return flow.Continue("Deployments of primary CN  are rolled out.")
		}
		return flow.RetryAfter(5*time.Second, "Some deployment of primary CN is rolling.")
	},
)

var WaitUntilCNPodsStable = polardbxv1reconcile.NewStepBinder("WaitUntilCNPodsStable",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		cnPods, err := rc.GetPods(polardbxmeta.RoleCN)
		if err != nil {
			return flow.Error(err, "Unable to get pods of CN.")
		}

		unFinalizedPodsSize := k8shelper.FilterPodsBy(cnPods, func(pod *corev1.Pod) bool {
			return len(pod.Finalizers) > 0
		})

		cnTemplate := &polardbx.Status.SpecSnapshot.Topology.Nodes.CN
		if len(unFinalizedPodsSize) == int(*cnTemplate.Replicas) {
			return flow.Pass()
		}
		return flow.Wait("Wait until some pod to be finalized.")
	},
)

var WaitUntilCDCPodsStable = polardbxv1reconcile.NewStepBinder("WaitUntilCDCPodsStable",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		if polardbx.Spec.Readonly {
			return flow.Pass()
		}

		cdcPods, err := rc.GetPods(polardbxmeta.RoleCDC)
		if err != nil {
			return flow.Error(err, "Unable to get pods of CDC.")
		}

		unFinalizedPodsSize := k8shelper.FilterPodsBy(cdcPods, func(pod *corev1.Pod) bool {
			return len(pod.Finalizers) > 0
		})

		cdcTemplate := polardbx.Status.SpecSnapshot.Topology.Nodes.CDC
		cdcReplicas := 0
		if cdcTemplate != nil {
			cdcReplicas = int(cdcTemplate.Replicas.IntValue() + cdcTemplate.XReplicas)
		}
		cdcNodes := polardbx.Spec.Topology.Nodes.CDC
		if cdcNodes != nil {
			for _, group := range cdcNodes.Groups {
				cdcReplicas = cdcReplicas + int(group.Replicas)
			}
		}
		if len(unFinalizedPodsSize) == cdcReplicas {
			return flow.Pass()
		}
		return flow.Wait("Wait until some pod to be finalized.")
	},
)

var WaitUntilCDCDeploymentsRolledOut = polardbxv1reconcile.NewStepBinder("WaitUntilCDCDeploymentsRolledOut",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		cdcDeployments, err := rc.GetDeploymentMap(polardbxmeta.RoleCDC)
		if err != nil {
			return flow.Error(err, "Unable to get deployments of CDC.")
		}

		if areDeploymentsRolledOut(cdcDeployments) {
			return flow.Continue("Deployments of CDC are rolled out.")
		}
		return flow.Wait("Some deployment of CDC is rolling.")
	},
)

var WaitUntilColumnarPodsStable = polardbxv1reconcile.NewStepBinder("WaitUntilColumnarPodsStable",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		columnarPods, err := rc.GetPods(polardbxmeta.RoleColumnar)
		if err != nil {
			return flow.Error(err, "Unable to get pods of Columnar.")
		}

		unFinalizedPodsSize := k8shelper.FilterPodsBy(columnarPods, func(pod *corev1.Pod) bool {
			return len(pod.Finalizers) > 0
		})

		columnarTemplate := polardbx.Status.SpecSnapshot.Topology.Nodes.Columnar
		columnarReplicas := 0
		if columnarTemplate != nil {
			columnarReplicas = int(columnarTemplate.Replicas)
		}
		if len(unFinalizedPodsSize) == columnarReplicas {
			return flow.Pass()
		}
		return flow.Wait("Wait until some pod to be finalized.")
	},
)

var WaitUntilColumnarDeploymentsRolledOut = polardbxv1reconcile.NewStepBinder("WaitUntilColumnarDeploymentsRolledOut",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		columnarDeployments, err := rc.GetDeploymentMap(polardbxmeta.RoleColumnar)
		if err != nil {
			return flow.Error(err, "Unable to get deployments of Columnar.")
		}

		if areDeploymentsRolledOut(columnarDeployments) {
			return flow.Continue("Deployments of Columnar are rolled out.")
		}
		return flow.Wait("Some deployment of Columnar is rolling.")
	},
)

var TrySyncCnLabelToPodsDirectly = polardbxv1reconcile.NewStepBinder("TrySyncCnLabelToPodsDirectly",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		changedCount := 0
		cnPods, err := rc.GetPods(polardbxmeta.RoleCN)
		if err != nil {
			return flow.Error(err, "Failed to Get Cn Pods")
		}
		for _, pod := range cnPods {
			if pod.Labels[polardbxmeta.LabelAuditLog] != strconv.FormatBool(rc.MustGetPolarDBX().Spec.Config.CN.EnableAuditLog) {
				pod.SetLabels(k8shelper.PatchLabels(pod.Labels, map[string]string{
					polardbxmeta.LabelAuditLog: strconv.FormatBool(rc.MustGetPolarDBX().Spec.Config.CN.EnableAuditLog),
				}))
				if err := rc.Client().Update(rc.Context(), &pod); err != nil {
					return flow.RetryErr(err, "Failed to Update pod for polardbx/enableAuditLog")
				}
				changedCount++
			}
		}
		return flow.Continue(" CN Deployment labels are synced", "count", changedCount)
	},
)

// add or set label 'enableAuditLog' to all dn pods according to pxc's spec.config.dn
var TrySyncDnLabelToPodsDirectly = polardbxv1reconcile.NewStepBinder("TrySyncDnLabelToPodsDirectly",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		changedCount := 0
		dnPods, err := rc.GetPods(polardbxmeta.RoleDN)
		if err != nil {
			return flow.Error(err, "Failed to Get Dn Pods")
		}
		for _, pod := range dnPods {
			if pod.Labels[polardbxmeta.LabelAuditLog] != strconv.FormatBool(rc.MustGetPolarDBX().Spec.Config.DN.EnableAuditLog) {
				pod.SetLabels(k8shelper.PatchLabels(pod.Labels, map[string]string{
					polardbxmeta.LabelAuditLog: strconv.FormatBool(rc.MustGetPolarDBX().Spec.Config.DN.EnableAuditLog),
				}))
				if err := rc.Client().Update(rc.Context(), &pod); err != nil {
					return flow.RetryErr(err, "Failed to Update pod for polardbx/enableAuditLog")
				}
				changedCount++
			}
		}
		return flow.Continue(" DN labels are synced", "count", changedCount)
	},
)

// GMS actually use DN's config, so this function adds/sets label 'enableAuditLog' to all gms pods according to pxc's spec.config.dn .
// This step is not merged into TrySyncDnLabelToPodsDirectly to make the logic clear.
var TrySyncGMSLabelToPodsDirectly = polardbxv1reconcile.NewStepBinder("TrySyncGMSLabelToPodsDirectly",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		changedCount := 0
		gmsPods, err := rc.GetPods(polardbxmeta.RoleGMS)
		if err != nil {
			return flow.Error(err, "Failed to Get GMS Pods")
		}
		for _, pod := range gmsPods {
			if pod.Labels[polardbxmeta.LabelAuditLog] != strconv.FormatBool(rc.MustGetPolarDBX().Spec.Config.DN.EnableAuditLog) {
				pod.SetLabels(k8shelper.PatchLabels(pod.Labels, map[string]string{
					polardbxmeta.LabelAuditLog: strconv.FormatBool(rc.MustGetPolarDBX().Spec.Config.DN.EnableAuditLog),
				}))
				if err := rc.Client().Update(rc.Context(), &pod); err != nil {
					return flow.RetryErr(err, "Failed to Update pod for polardbx/enableAuditLog")
				}
				changedCount++
			}
		}
		return flow.Continue(" GMS labels are synced", "count", changedCount)
	},
)

var WaitUntilDNsTdeReady = polardbxv1reconcile.NewStepBinder("WaitUntilDNsReady",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		topology := &polardbx.Status.SpecSnapshot.Topology

		dnStores, err := rc.GetDNMap()
		if err != nil {
			return flow.Error(err, "Unable to list xstores of DNs.")
		}

		notReadyCnt, skipCnt := 0, 0
		for i, dnStore := range dnStores {
			// Trailing DNs.
			if i >= int(topology.Nodes.DN.Replicas) {
				skipCnt++
				continue
			}

			if dnStore.Status.Phase == polardbxv1xstore.PhaseFailed {
				helper.TransferPhase(rc.MustGetPolarDBX(), polardbxv1polardbx.PhaseFailed)
				return flow.Retry("XStore of DN is failed, transfer phase into failed.", "xstore", dnStore.Name)
			}

			if !isXStoreTdeReady(dnStore) {
				notReadyCnt++
			}
		}

		if notReadyCnt > 0 {
			return flow.Wait("Some xstore of DN is not ready, wait.", "not-ready", notReadyCnt, "skip", skipCnt)
		}

		return flow.Continue("XStores of DNs are ready.", "skip", skipCnt)
	},
)

func isXStoreTdeReady(xstore *polardbxv1.XStore) bool {
	return xstore.Status.ObservedGeneration == xstore.Generation &&
		xstore.Status.Phase == polardbxv1xstore.PhaseRunning && xstore.Status.TdeStatus && !xstore.Status.Restarting
}
