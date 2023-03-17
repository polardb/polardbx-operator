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
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TransferPhaseTo(phase polardbxv1.BackupBinlogPhase, requeue ...bool) control.BindFunc {
	return polardbxv1reconcile.NewStepBinder("TransferPhaseTo"+string(phase),
		func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			backupBinlog := rc.MustGetPolarDBXBackupBinlog()
			backupBinlog.Status.Phase = phase
			rc.MarkPolarDBXChanged()
			if len(requeue) == 0 || !requeue[0] {
				return flow.Pass()
			} else {
				return flow.Retry("Phase updated!", "target-phase", phase)
			}
		},
	)
}

var PersistentBackupBinlog = polardbxv1reconcile.NewStepBinder("PersistentBackupBinlog",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsPolarDBXChanged() {
			if err := rc.UpdatePolarDbXBackupBinlog(); err != nil {
				return flow.Error(err, "Unable to persistent polardbx backup binlog.")
			}
			return flow.Continue("Succeeds to persistent polardbx backup binlog.")
		}
		return flow.Continue("Object not changed.")
	})

func WhenDeleting(binders ...control.BindFunc) control.BindFunc {
	return polardbxv1reconcile.NewStepIfBinder("Deleted",
		func(rc *polardbxv1reconcile.Context, log logr.Logger) (bool, error) {
			backupBinlog := rc.MustGetPolarDBXBackupBinlog()
			if backupBinlog.Status.Phase == polardbxv1.BackupBinlogPhaseDeleting {
				return false, nil
			}
			deleting := !backupBinlog.DeletionTimestamp.IsZero()
			return deleting, nil
		},
		binders...,
	)
}

var UpdateObservedGeneration = polardbxv1reconcile.NewStepBinder("UpdateObservedGeneration",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backupBinlog := rc.MustGetPolarDBXBackupBinlog()
		prevGen := backupBinlog.Status.ObservedGeneration
		backupBinlog.Status.ObservedGeneration = backupBinlog.Generation
		return flow.Continue("Update observed generation.", "previous-generation", prevGen,
			"current-generation", backupBinlog.Generation)
	},
)
