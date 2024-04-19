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
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var AddFinalizer = polardbxv1reconcile.NewStepBinder("AddFinalizer",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		if !controllerutil.ContainsFinalizer(backup, polardbxmeta.Finalizer) {
			controllerutil.AddFinalizer(backup, polardbxmeta.Finalizer)
			rc.MarkPolarDBXChanged()
			return flow.Continue("Finalizer added.")
		}
		return flow.Pass()
	})

var RemoveFinalizer = polardbxv1reconcile.NewStepBinder("RemoveFinalizer",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		if controllerutil.ContainsFinalizer(backup, polardbxmeta.Finalizer) {
			controllerutil.RemoveFinalizer(backup, polardbxmeta.Finalizer)
			rc.MarkPolarDBXChanged()
			return flow.Continue("Finalizer removed.")
		}
		return flow.Pass()
	})
