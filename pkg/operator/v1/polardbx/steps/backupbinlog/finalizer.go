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

package backupbinlog

import (
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var AddFinalizer = polardbxv1reconcile.NewStepBinder("AddFinalizer", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	if controllerutil.ContainsFinalizer(backupBinlog, meta.Finalizer) {
		return flow.Pass()
	}
	controllerutil.AddFinalizer(backupBinlog, meta.Finalizer)
	rc.MarkPolarDBXChanged()
	return flow.Continue("Add finalizer.")
})

var RemoveFinalizer = polardbxv1reconcile.NewStepBinder("RemoveFinalizer", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	if !controllerutil.ContainsFinalizer(backupBinlog, meta.Finalizer) {
		return flow.Pass()
	}
	controllerutil.RemoveFinalizer(backupBinlog, meta.Finalizer)
	rc.MarkPolarDBXChanged()
	return flow.Continue("Remove finalizer.")
})
