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
	"sort"

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
		accountSecret, err := rc.GetPolarDBXSecret(convention.SecretTypeAccount)
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get account secret.")
		}
		if accountSecret == nil {
			accountSecret, err = factory.NewObjectFactory(rc).NewSecret()
			if err != nil {
				return flow.Error(err, "Unable to new account secret.")
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

		for _, serviceType := range []convention.ServiceType{
			convention.ServiceTypeReadWrite,
			convention.ServiceTypeReadOnly,
			convention.ServiceTypeCDCMetrics,
		} {
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

var CreateOrReconcileDNs = polardbxv1reconcile.NewStepBinder("CreateOrReconcileDNs",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
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
		for i := 0; i < replicas; i++ {
			observedDnStore, ok := dnStores[i]

			if !ok {
				newDnStore, err := objectFactory.NewXStoreDN(i)
				if err != nil {
					return flow.Error(err, "Unable to new xstore of DN.", "index", i)
				}

				err = rc.SetControllerRefAndCreate(newDnStore)
				if err != nil {
					return flow.Error(err, "Unable to create xstore of DN.", "index", i)
				}
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

					convention.CopyMetadataForUpdate(&newDnStore.ObjectMeta, &observedDnStore.ObjectMeta, observedGeneration)

					err = rc.Client().Update(rc.Context(), newDnStore)
					if err != nil {
						return flow.Error(err, "Unable to update xstore of DN.", "index", i)
					}
					anyChanged = true
				}
			}
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
			if initialized, err := mgr.IsMetaDBInitialized(); err != nil {
				return flow.Error(err, "Unable to determine if GMS is initialized.")
			} else if initialized {
				storageNodes, err := mgr.ListStorageNodes(gms.StorageKindMaster)
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

func reconcileGroupedDeployments(rc *polardbxv1reconcile.Context, flow control.Flow, role string) (reconcile.Result, error) {
	polardbxmeta.AssertRoleIn(role, polardbxmeta.RoleCN, polardbxmeta.RoleCDC)

	flow = flow.WithLoggerValues("role", role)

	polardbx := rc.MustGetPolarDBX()
	observedGeneration := polardbx.Status.ObservedGeneration

	observedDeployments, err := rc.GetDeploymentMap(role)
	if err != nil {
		return flow.Error(err, "Unable to get deployments.")
	}

	var deployments map[string]appsv1.Deployment
	if role == polardbxmeta.RoleCN {
		deployments, err = factory.NewObjectFactory(rc).NewDeployments4CN()
	} else {
		deployments, err = factory.NewObjectFactory(rc).NewDeployments4CDC()
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
				convention.CopyMetadataForUpdate(&newDeployment.ObjectMeta, &observedDeployment.ObjectMeta,
					observedGeneration)
				err := rc.Client().Update(rc.Context(), &newDeployment)
				if err != nil {
					return flow.Error(err, "Unable to update deployment.",
						"deployment", observedDeployment.Name)
				}
				anyChanged = true
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

func isXStoreReady(xstore *polardbxv1.XStore) bool {
	return xstore.Status.ObservedGeneration == xstore.Generation &&
		xstore.Status.Phase == polardbxv1xstore.PhaseRunning
}

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
		if len(unFinalizedPodsSize) == int(cnTemplate.Replicas) {
			return flow.Pass()
		}
		return flow.Wait("Wait until some pod to be finalized.")
	},
)

var WaitUntilCDCPodsStable = polardbxv1reconcile.NewStepBinder("WaitUntilCDCPodsStable",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		cdcPods, err := rc.GetPods(polardbxmeta.RoleCDC)
		if err != nil {
			return flow.Error(err, "Unable to get pods of CN.")
		}

		unFinalizedPodsSize := k8shelper.FilterPodsBy(cdcPods, func(pod *corev1.Pod) bool {
			return len(pod.Finalizers) > 0
		})

		cdcTemplate := polardbx.Status.SpecSnapshot.Topology.Nodes.CDC
		cdcReplicas := 0
		if cdcTemplate != nil {
			cdcReplicas = int(cdcTemplate.Replicas)
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
