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

package finalizer

import (
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	reconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
)

var SetupGuardFinalizer = polardbxv1reconcile.NewStepBinder("SetupGuardFinalizer",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if !controllerutil.ContainsFinalizer(polardbx, polardbxmeta.Finalizer) {
			controllerutil.AddFinalizer(polardbx, polardbxmeta.Finalizer)
			rc.MarkPolarDBXChanged()
			return flow.Continue("Setup guard finalizer.")
		}
		return flow.Pass()
	},
)

var RemoveGuardFinalizer = polardbxv1reconcile.NewStepBinder("RemoveGuardFinalizer",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if controllerutil.ContainsFinalizer(polardbx, polardbxmeta.Finalizer) {
			controllerutil.RemoveFinalizer(polardbx, polardbxmeta.Finalizer)
			rc.MarkPolarDBXChanged()
			return flow.Continue("Remove guard finalizer.")
		}
		return flow.Pass()
	},
)

var BlockBeforeOtherSystemsFinalized = polardbxv1reconcile.NewStepBinder("BlockBeforeOtherSystemsFinalized",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if k8shelper.ContainsOnlyFinalizer(polardbx, polardbxmeta.Finalizer) {
			return flow.Pass()
		}
		return flow.Wait("Block, other finalizers detected.")
	},
)
