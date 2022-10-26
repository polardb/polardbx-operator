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
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var AddFinalizer = NewStepBinder("AddFinalizer", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xStoreFollower := rc.MustGetXStoreFollower()
	if controllerutil.ContainsFinalizer(xStoreFollower, xstoremeta.Finalizer) {
		return flow.Pass()
	}

	controllerutil.AddFinalizer(xStoreFollower, xstoremeta.Finalizer)
	rc.MarkChanged()
	return flow.Continue("Add finalizer.")
})

var RemoveFinalizer = NewStepBinder("RemoveFinalizer", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xStoreFollower := rc.MustGetXStoreFollower()
	if !controllerutil.ContainsFinalizer(xStoreFollower, xstoremeta.Finalizer) {
		return flow.Pass()
	}
	controllerutil.RemoveFinalizer(xStoreFollower, xstoremeta.Finalizer)
	rc.MarkChanged()
	return flow.Continue("Remove finalizer.")
})

var CheckDeletionStatusAndRedirect = NewStepBinder("CheckDeletionStatusAndRedirect", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xStoreFollower := rc.MustGetXStoreFollower()
	if xStoreFollower.Status.Phase == polardbxv1xstore.FollowerPhaseDeleting {
		return flow.Pass()
	}
	if !xStoreFollower.DeletionTimestamp.IsZero() {
		// Only move to deleting when other finalizers have been removed.
		if len(xStoreFollower.Finalizers) == 0 ||
			(len(xStoreFollower.Finalizers) == 1 &&
				controllerutil.ContainsFinalizer(xStoreFollower, xstoremeta.Finalizer)) {
			xStoreFollower.Status.Phase = polardbxv1xstore.FollowerPhaseDeleting
			rc.MarkChanged()
			return flow.Retry("Move phase to deleting. Retry immediately!")
		} else {
			return flow.Wait("Other finalizers found, wait until removed...")
		}
	}
	return flow.Continue("Removed finalizer.")
})
