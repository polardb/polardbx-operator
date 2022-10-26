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
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strings"
)

func TransferPhaseTo(phase polardbxv1.PolarDBXBackupPhase, requeue ...bool) control.BindFunc {
	return polardbxv1reconcile.NewStepBinder("TransferPhaseTo"+string(phase),
		func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			polardbxBackup := rc.MustGetPolarDBXBackup()
			polardbxBackup.Status.Phase = phase
			if len(requeue) == 0 || !requeue[0] {
				return flow.Pass()
			} else {
				return flow.Retry("Phase updated!", "target-phase", phase)
			}
		},
	)
}

func GenerateJobName(pxcBackup *polardbxv1.PolarDBXBackup, JobLabel string) string {
	// 理论情况下, jobName不应该超过63位, 并且在每次job完成后，我们会将job删除，所以这里应该不会出现同时job名称冲突的情况.
	jobName := JobLabel + "-job-" + pxcBackup.Name + "-" + rand.String(4)
	if len(jobName) >= 60 {
		jobName = jobName[0:59]
		jobName = strings.TrimRight(jobName, "-")
	}
	return jobName
}
