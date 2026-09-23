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

package steps

import (
	"github.com/alibaba/polardbx-operator/api/v1/systemtask"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/systemtask/common"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var PersistentSystemTask = common.NewStepBinder("PersistentSystemTask",
	func(rc *common.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsSystemTaskChanged() {
			if err := rc.UpdateSystemTask(); err != nil {
				return flow.Error(err, "Unable to persistent system task.")
			}
			return flow.Continue("Succeeds to persistent system task.")
		}
		return flow.Continue("Object not changed.")
	})

func TransferPhaseTo(phase systemtask.Phase, requeue bool) control.BindFunc {
	return common.NewStepBinder("TransferPhaseTo"+string(phase),
		func(rc *common.Context, flow control.Flow) (reconcile.Result, error) {
			systemTask := rc.MustGetSystemTask()
			systemTask.Status.Phase = phase
			rc.MarkSystemTaskChanged()
			if requeue {
				return flow.Retry("Retry immediately.")
			}
			return flow.Pass()
		},
	)
}
