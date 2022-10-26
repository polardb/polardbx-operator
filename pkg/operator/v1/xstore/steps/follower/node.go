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
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// check if the node exists

var CheckNode = NewStepBinder("CheckNode",
	func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
		xstoreFollower := rc.MustGetXStoreFollower()
		if xstoreFollower.Spec.NodeName == "" {
			return flow.Pass()
		}
		objKey := types.NamespacedName{
			Namespace: "",
			Name:      xstoreFollower.Spec.NodeName,
		}
		node := corev1.Node{}
		err := rc.Client().Get(rc.Context(), objKey, &node)
		if err != nil {
			xstoreFollower.Status.Phase = xstorev1.FollowerPhaseFailed
			msg := fmt.Sprintf("Failed to check NodeName %s", objKey.Name)
			xstoreFollower.Status.Message = msg
			rc.MarkChanged()
			return flow.Error(err, "CheckNode Failed"+" "+msg)
		}
		return flow.Continue("TryLoadFromPod Success.")
	})
