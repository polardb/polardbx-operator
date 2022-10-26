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

package follower

import (
	polarxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstorereconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func UpdatePhaseTemplate(phase polardbxv1xstore.FollowerPhase) control.BindFunc {
	return NewStepBinder("UpdatePhaseTo"+string(phase),
		func(rc *xstorereconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
			xstoreFollower, err := rc.GetXStoreFollower()
			if err != nil {
				return flow.Error(err, "Unable to get xstore.")
			}
			xstoreFollower.Status.Phase = phase
			rc.MarkChanged()
			return flow.Retry("Phase updated!", "target-phase", phase)
		})
}

var PersistentXStoreFollower = NewStepBinder("PersistentXStoreFollower",
	func(rc *xstorereconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
		if rc.IsChanged() {
			if err := rc.UpdateXStoreFollower(); err != nil {
				return flow.RetryErr(err, "Unable to persistent xstore follower.")
			}
			return flow.Continue("Succeeds to persistent xstore follower.")
		}
		return flow.Continue("Object not changed.")
	})

func HasFlowFlags(rc *xstorereconcile.FollowerContext, flowFlagName string) bool {
	xstoreFollower := rc.MustGetXStoreFollower()
	if xstoreFollower.Status.FlowFlags == nil {
		return false
	}
	for _, val := range xstoreFollower.Status.FlowFlags {
		if val.Name == flowFlagName && val.Value {
			return true
		}
	}
	return false
}

func SetFlowFlags(rc *xstorereconcile.FollowerContext, flowFlagName string, flowFlagValue bool) {
	xstoreFollower := rc.MustGetXStoreFollower()
	if xstoreFollower.Status.FlowFlags == nil {
		xstoreFollower.Status.FlowFlags = make([]polarxv1.FlowFlagType, 0)
	}
	xstoreFollower.Status.FlowFlags = append(xstoreFollower.Status.FlowFlags, polarxv1.FlowFlagType{
		Name:  flowFlagName,
		Value: flowFlagValue,
	})
	rc.MarkChanged()
}
