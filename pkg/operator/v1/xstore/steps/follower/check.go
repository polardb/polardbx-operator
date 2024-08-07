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
	"fmt"
	polarxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polarxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func IsNotLogger(xstoreFollower *polarxv1.XStoreFollower) bool {
	result := xstoreFollower.Spec.Role != polarxv1xstore.FollowerRoleLogger
	return result
}

func IsTdeOpen(xstore *polarxv1.XStore) bool {
	role := xstore.Labels[polardbxmeta.LabelRole]
	if role != polardbxmeta.RoleGMS && xstore.Spec.TDE.Enable {
		return true
	}
	return false
}

//check if the xstore exists

var CheckXStore = NewStepBinder("CheckXStore",
	func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
		_, err := rc.GetXStore()
		xStoreFollower := rc.MustGetXStoreFollower()
		if err != nil {
			xStoreFollower.Status.Message = fmt.Sprintf("xstore %s does not exist", xStoreFollower.Spec.XStoreName)
			rc.MarkChanged()
			flow.Wait("WaitUntil xstore is being")
		}
		if xStoreFollower.GetLabels() == nil {
			xStoreFollower.SetLabels(map[string]string{})
		}
		xStoreFollower.SetLabels(k8shelper.PatchLabels(xStoreFollower.GetLabels(), map[string]string{
			xstoremeta.LabelName: xStoreFollower.Spec.XStoreName,
		}))
		return flow.Continue("CheckXStore success.")
	})
