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

	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/alibaba/polardbx-operator/pkg/util/unit"
)

var PersistentStatus = xstorev1reconcile.NewStepBinder("PersistentStatus",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsXStoreStatusChanged() {
			if err := rc.UpdateXStoreStatus(); err != nil {
				return flow.Error(err, "Unable to persistent status.")
			}
			return flow.Continue("Succeeds to persistent status.")
		}
		return flow.Continue("Status not changed.")
	})

var PersistentXStore = xstorev1reconcile.NewStepBinder("PersistentXStore",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsXStoreChanged() {
			if err := rc.UpdateXStore(); err != nil {
				return flow.Error(err, "Unable to persistent xstore.")
			}
			return flow.Continue("Succeeds to persistent xstore.")
		}
		return flow.Continue("Object not changed.")
	})

func UpdatePhaseTemplate(phase polardbxv1xstore.Phase, requeue ...bool) control.BindFunc {
	return xstorev1reconcile.NewStepBinder("UpdatePhaseTo"+string(phase),
		func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			xstore, err := rc.GetXStore()
			if err != nil {
				return flow.Error(err, "Unable to get xstore.")
			}
			xstore.Status.Stage = polardbxv1xstore.StageEmpty
			xstore.Status.Phase = phase
			if len(requeue) == 0 || !requeue[0] {
				return flow.Continue("Phase updated!", "target-phase", phase)
			} else {
				return flow.Retry("Phase updated!", "target-phase", phase)
			}
		})
}

func UpdateStageTemplate(stage polardbxv1xstore.Stage, requeue ...bool) control.BindFunc {
	return xstorev1reconcile.NewStepBinder("UpdateStageTo"+string(stage),
		func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			xstore, err := rc.GetXStore()
			if err != nil {
				return flow.Error(err, "Unable to get xstore.")
			}
			xstore.Status.Stage = stage
			if len(requeue) == 0 || !requeue[0] {
				return flow.Continue("Stage updated!", "target-stage", stage)
			} else {
				return flow.Retry(
					"Stage changed!", "target-stage", stage)
			}
		})
}

var MoveToPhaseDeletingIfDeleted = xstorev1reconcile.NewStepBinder("MoveToPhaseDeletingIfDeleted",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore, err := rc.GetXStore()
		if err != nil {
			return flow.Error(err, "Unable to get xstore.")
		}

		if xstore.Status.Phase == polardbxv1xstore.PhaseDeleting {
			return flow.Pass()
		}

		if !xstore.DeletionTimestamp.IsZero() {
			// Only move to deleting when other finalizers have been removed.
			if len(xstore.Finalizers) == 0 ||
				(len(xstore.Finalizers) == 1 &&
					controllerutil.ContainsFinalizer(xstore, xstoremeta.Finalizer)) {
				xstore.Status.Phase = polardbxv1xstore.PhaseDeleting
				xstore.Status.Stage = polardbxv1xstore.StageEmpty
				return flow.Retry("Move phase to deleting. Retry immediately!")
			} else {
				return flow.Wait("Other finalizers found, wait until removed...")
			}
		}

		return flow.Pass()
	})

var UpdateDisplayStatus = xstorev1reconcile.NewStepBinder("UpdateDisplayStatus",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		status := &xstore.Status

		// Count total necessary pods
		topology := status.ObservedTopology

		if topology != nil {
			status.TotalPods = 0
			for _, t := range topology.NodeSets {
				status.TotalPods += t.Replicas
			}
		}

		// Count ready pods
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		status.ReadyPods = 0
		for _, po := range pods {
			if k8shelper.IsPodReady(&po) {
				status.ReadyPods++
			}
		}

		// Refill the ready status
		status.ReadyStatus = fmt.Sprintf("%d/%d", status.ReadyPods, status.TotalPods)

		// Calculate the total size.
		if status.BoundVolumes != nil {
			totalDataDirSize := int64(0)
			for _, v := range status.BoundVolumes {
				if v != nil {
					totalDataDirSize += v.Size
				}
			}
			status.TotalDataDirSize = unit.ByteCountIEC(totalDataDirSize)
		}

		return flow.Continue("Display status updated!")
	})
