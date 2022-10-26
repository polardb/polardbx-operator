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
	"errors"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/galaxy"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

// Deprecated
var DummyReconcileConsensusRoleLabels = plugin.NewStepBinder(galaxy.Engine, "ReconcileConsensusRoleLabels",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		candidatePods := k8shelper.FilterPodsBy(pods, func(pod *corev1.Pod) bool {
			return xstoremeta.IsPodRoleCandidate(pod)
		})

		if len(candidatePods) > 1 {
			return flow.Error(errors.New("must have only one candidate"), "Found multiple candidate pods, which is not supported yet...")
		}

		// Try to reconcile the label.
		currentLeader := xstore.Status.LeaderPod

		candidatePods = k8shelper.FilterPodsBy(candidatePods, k8shelper.IsPodReady)
		if len(candidatePods) > 0 {
			candidatePod := &candidatePods[0]
			currentLeader = candidatePod.Name
			if candidatePod.Labels[xstoremeta.LabelRole] != xstoremeta.RoleLeader {
				candidatePod.Labels[xstoremeta.LabelRole] = xstoremeta.RoleLeader
				err := rc.Client().Update(rc.Context(), candidatePod)
				if err != nil {
					flow.Logger().Error(err, "Unable to reconcile label of node role",
						"pod", candidatePod.Name, "role", xstoremeta.RoleLeader)
				}
			}
		} else {
			currentLeader = ""
		}

		if len(currentLeader) == 0 {
			xstore.Status.LeaderPod = ""

			rc.UpdateXStoreCondition(&polardbxv1xstore.Condition{
				Type:    polardbxv1xstore.LeaderReady,
				Status:  corev1.ConditionFalse,
				Reason:  "LeaderNotFound",
				Message: "Leader not found",
			})

			return flow.Continue("Leader not found!")
		} else if currentLeader != xstore.Status.LeaderPod {
			xstore.Status.LeaderPod = currentLeader

			rc.UpdateXStoreCondition(&polardbxv1xstore.Condition{
				Type:    polardbxv1xstore.LeaderReady,
				Status:  corev1.ConditionTrue,
				Reason:  "LeaderFound",
				Message: "Leader found: " + currentLeader,
			})

			return flow.Continue("Leader changed!", "leader-pod", currentLeader)
		} else {
			xstore.Status.LeaderPod = currentLeader

			rc.UpdateXStoreCondition(&polardbxv1xstore.Condition{
				Type:    polardbxv1xstore.LeaderReady,
				Status:  corev1.ConditionTrue,
				Reason:  "LeaderFound",
				Message: "Leader found: " + currentLeader,
			})

			return flow.Continue("Leader not changed.", "leader-pod", currentLeader)
		}
	})
