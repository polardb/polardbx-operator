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
	"time"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	xstoreexec "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

var CreateAccounts = xstorev1reconcile.NewStepBinder("CreateAccounts",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.Error(err, "Unable to get leader pod.")
		}

		// Branch leader not found, just wait for 5 seconds and retry again.
		if leaderPod == nil {
			return flow.RetryAfter(5*time.Second, "Leader not found, wait 5 seconds and retry...")
		}

		secret, err := rc.GetXStoreSecret()
		if err != nil {
			return flow.Error(err, "Unable to get secret.")
		}

		// Create accounts one-by-one
		for user, passwd := range secret.Data {
			cmd := xstoreexec.NewCanonicalCommandBuilder().
				Account().Create(user, string(passwd)).
				Build()

			err := rc.ExecuteCommandOn(leaderPod, "engine", cmd, control.ExecOptions{
				Logger: flow.Logger(),
			})

			if err != nil {
				// Branch it's an exit error (which means commands already executed and returned),
				// give it a chance to retry.
				if k8shelper.IsExitError(err) {
					flow.Logger().Error(err, "Failed to create account.")
					return flow.Wait("Failed to create account.", "leader-pod", leaderPod.Name)
				}
				return flow.Error(err, "Unable to create account.", "leader-pod", leaderPod.Name)
			}
		}

		return flow.Continue("All accounts are created.")
	},
)
