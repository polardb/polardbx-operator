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

package instance

import (
	v1 "github.com/alibaba/polardbx-operator/api/v1"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/galaxy"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func PurgeLogsTemplate(d time.Duration) control.BindFunc {
	if d.Minutes() == 0 {
		panic("invalid interval")
	}

	return plugin.NewStepBinder(galaxy.Engine, "PurgeLogs",
		func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {

			backupBinlogList := v1.PolarDBXBackupBinlogList{}
			err := rc.Client().List(rc.Context(), &backupBinlogList)
			if err != nil {
				return flow.RetryErr(err, "failed to get backup binlog list")
			}
			if len(backupBinlogList.Items) > 0 {
				return flow.Pass()
			}

			xstore := rc.MustGetXStore()

			// if Purge binlog Locked
			if xstore.Labels[xstoremeta.LabelBinlogPurgeLock] == xstoremeta.BinlogPurgeLock {
				return flow.Pass()
			}

			// avoid duplicated purge request from learner
			if xstore.Spec.Readonly {
				return flow.Pass()
			}

			creatingLearners, err := rc.CountCreatingLearners()

			if err != nil {
				flow.Logger().Error(err, "failed to count creating learners.")
			}

			if creatingLearners > 0 {
				return flow.Continue("There are still learner creating, purge cancelled.")
			}

			timeToPurge := xstore.Status.LastLogPurgeTime.IsZero() || // It's the first time.
				xstore.Status.LastLogPurgeTime.Add(d).Before(time.Now()) // A duration of given interval has elapsed since last purge.

			if !timeToPurge {
				return flow.Pass()
			}

			// Purge binary logs on leader (the only) pod.

			leaderPod, err := rc.TryGetXStoreLeaderPod()
			if err != nil {
				return flow.Error(err, "Unable to get leader pod.")
			}
			if leaderPod == nil {
				return flow.Wait("No leader pod found, must wait for another reconciliation.")
			}

			purgeCmd := command.NewCanonicalCommandBuilder().Consensus().PurgeLogs(false, false).Build()
			err = rc.ExecuteCommandOn(leaderPod, convention.ContainerEngine, purgeCmd, control.ExecOptions{
				Logger:  flow.Logger(),
				Timeout: 2 * time.Second,
			})
			if err != nil {
				return flow.Error(err, "Unable to purge logs.")
			}

			// update purge time.
			xstore.Status.LastLogPurgeTime = k8shelper.TimePtr(metav1.Now())

			return flow.Pass()
		},
	)
}
