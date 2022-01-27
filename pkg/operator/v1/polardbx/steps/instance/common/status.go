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
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"github.com/alibaba/polardbx-operator/pkg/util/unit"
)

func TransferPhaseTo(phase polardbxv1polardbx.Phase, requeue bool) control.BindFunc {
	return polardbxv1reconcile.NewStepBinder("TransferPhaseTo"+string(phase),
		func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			polardbx := rc.MustGetPolarDBX()
			polardbx.Status.Stage = polardbxv1polardbx.StageEmpty
			polardbx.Status.Phase = phase
			if requeue {
				return flow.Retry("Retry immediately.")
			}
			return flow.Pass()
		},
	)
}

func TransferStageTo(stage polardbxv1polardbx.Stage, requeue bool) control.BindFunc {
	return polardbxv1reconcile.NewStepBinder("TransferStageTo"+string(stage),
		func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			polardbx := rc.MustGetPolarDBX()
			polardbx.Status.Stage = stage
			if requeue {
				return flow.Retry("Retry immediately.")
			}
			return flow.Pass()
		},
	)
}

func WhenDeletedAndNotDeleting(binders ...control.BindFunc) control.BindFunc {
	return polardbxv1reconcile.NewStepIfBinder("Deleted",
		func(rc *polardbxv1reconcile.Context, log logr.Logger) (bool, error) {
			polardbx := rc.MustGetPolarDBX()
			if helper.IsPhaseIn(polardbx, polardbxv1polardbx.PhaseDeleting) {
				return false, nil
			}
			return !polardbx.DeletionTimestamp.IsZero(), nil
		},
		binders...,
	)
}

var UpdateSnapshotAndObservedGeneration = polardbxv1reconcile.NewStepBinder("UpdateSnapshotAndObservedGeneration",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		polardbx.Status.SpecSnapshot = &polardbxv1polardbx.SpecSnapshot{
			Topology: *polardbx.Spec.Topology.DeepCopy(),
			Config:   *polardbx.Spec.Config.DeepCopy(),
		}
		polardbx.Status.ObservedGeneration = polardbx.Generation
		return flow.Pass()
	},
)

func countAvailableXStores(xstores ...*polardbxv1.XStore) int {
	cnt := 0
	for _, xs := range xstores {
		if xs != nil && xs.Status.Phase == polardbxv1xstore.PhaseRunning {
			cnt += 1
		}
	}
	return cnt
}

func countAvailableReplicasFromDeployments(deployments map[string]*appsv1.Deployment) int {
	cnt := 0
	for _, d := range deployments {
		cnt += int(d.Status.AvailableReplicas)
	}
	return cnt
}

func getTotalDataSizeOfXStore(xstore *polardbxv1.XStore) int64 {
	totalSize := int64(0)
	for _, v := range xstore.Status.BoundVolumes {
		totalSize += v.Size
	}
	return totalSize
}

var UpdateDisplayStorageSize = polardbxv1reconcile.NewStepBinder("UpdateDisplayStorageSize",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		gmsStore, err := rc.GetGMS()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get xstore of GMS.")
		}
		dnStores, err := rc.GetOrderedDNList()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}

		now := time.Now()
		statusForPrintRef := &polardbx.Status.StatusForPrint

		// update storage size per minute.
		if statusForPrintRef.StorageSizeUpdateTime == nil ||
			statusForPrintRef.StorageSizeUpdateTime.Time.Add(1*time.Minute).Before(now) {
			totalSize := int64(0)
			if !polardbx.Spec.ShareGMS && gmsStore != nil {
				totalSize += getTotalDataSizeOfXStore(gmsStore)
			}
			for _, dnStore := range dnStores {
				totalSize += getTotalDataSizeOfXStore(dnStore)
			}
			statusForPrintRef.StorageSizeUpdateTime = &metav1.Time{
				Time: now,
			}
			statusForPrintRef.StorageSize = unit.ByteCountIEC(totalSize)

			return flow.Continue("Storage size updated!")
		}

		return flow.Pass()
	})

var UpdateDisplayReplicas = polardbxv1reconcile.NewStepBinder("UpdateDisplayReplicas",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		statusRef := &polardbx.Status

		// Ignore display update on initializing.
		if statusRef.ObservedGeneration <= 0 {
			return flow.Pass()
		}

		statusForPrintRef, snapshot := &polardbx.Status.StatusForPrint, polardbx.Status.SpecSnapshot

		cnDeployments, err := rc.GetDeploymentMap(polardbxmeta.RoleCN)
		if err != nil {
			return flow.Error(err, "Unable to get deployments of CN.")
		}
		gmsStore, err := rc.GetGMS()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get xstore of GMS.")
		}
		dnStores, err := rc.GetOrderedDNList()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}

		// update replicas status.
		statusRef.ReplicaStatus = polardbxv1polardbx.ClusterReplicasStatus{
			GMS: polardbxv1polardbx.ReplicasStatus{Total: 1, Available: int32(countAvailableXStores(gmsStore))},
			CN:  polardbxv1polardbx.ReplicasStatus{Total: snapshot.Topology.Nodes.CN.Replicas, Available: int32(countAvailableReplicasFromDeployments(cnDeployments))},
			DN:  polardbxv1polardbx.ReplicasStatus{Total: snapshot.Topology.Nodes.DN.Replicas, Available: int32(countAvailableXStores(dnStores...))},
			CDC: nil,
		}
		if snapshot.Topology.Nodes.CDC != nil {
			cdcDeployments, err := rc.GetDeploymentMap(polardbxmeta.RoleCDC)
			if err != nil {
				return flow.Error(err, "Unable to get deployments of CDC.")
			}
			statusRef.ReplicaStatus.CDC = &polardbxv1polardbx.ReplicasStatus{
				Available: int32(countAvailableReplicasFromDeployments(cdcDeployments)),
				Total:     snapshot.Topology.Nodes.CDC.Replicas,
			}
		}
		statusForPrintRef.ReplicaStatus = polardbxv1polardbx.ReplicaStatusForPrint{
			GMS: statusRef.ReplicaStatus.GMS.Display(),
			CN:  statusRef.ReplicaStatus.CN.Display(),
			DN:  statusRef.ReplicaStatus.DN.Display(),
			CDC: statusRef.ReplicaStatus.CDC.Display(),
		}

		return flow.Pass()
	},
)

var UpdateDisplayDetailedVersion = polardbxv1reconcile.NewStepBinder("UpdateDisplayDetailedVersion",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		mgr, err := rc.GetPolarDBXGroupManager()
		if err != nil {
			return flow.Error(err, "Unable to get group manager.")
		}
		clusterVersion, err := mgr.GetClusterVersion()
		if err != nil {
			return flow.Error(err, "Unable to get cluster version.")
		}
		polardbx.Status.StatusForPrint.DetailedVersion = clusterVersion
		return flow.Pass()
	},
)

var GenerateRandInStatus = polardbxv1reconcile.NewStepBinder("GenerateRandInStatus",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if len(polardbx.Status.Rand) == 0 {
			polardbx.Status.Rand = rand.String(4)
		}
		return flow.Pass()
	},
)
