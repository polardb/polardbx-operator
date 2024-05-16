/*
Copyright 2022 Alibaba Group Holding Limited.

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

package exec

import (
	"bytes"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

type DeleteExec struct {
	baseExec
}

func (exec *DeleteExec) Execute(
	rc *xstorev1reconcile.Context,
	flow control.Flow,
) (reconcile.Result, error) {
	step := exec.Step()
	target := exec.Step().Target

	// Check if the pod is deleted.
	pod, err := rc.GetXStorePod(exec.Step().Target)
	if err != nil {
		if apierrors.IsNotFound(err) {
			exec.MarkDone()
			return flow.Pass()
		} else {
			return flow.Error(err, "Failed to get pod", "pod", exec.Step().Target)
		}
	}

	// Detach from the paxos group.
	if exec.ec.ContainsPaxosNode(target) {
		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.Error(err, "Failed to get leader pod")
		}
		if leaderPod == nil {
			return flow.RetryAfter(5*time.Second, "Leader pod not found")
		}

		cmd := command.NewCanonicalCommandBuilder().Consensus().DropNode(target, step.TargetRole).Build()
		buf := &bytes.Buffer{}
		if err := rc.ExecuteCommandOn(leaderPod, xstoreconvention.ContainerEngine, cmd, control.ExecOptions{
			Logger:  flow.Logger(),
			Stdin:   nil,
			Stdout:  buf,
			Stderr:  buf,
			Timeout: 5 * time.Second,
		}); err != nil {
			return flow.Error(err, "Failed to drop consensus node", "pod", target)
		}

		exec.ec.RemovePaxosNode(target)
	}

	// TODO, recycle volumes and other resources before deleted.

	// Delete the pod.
	if pod.DeletionTimestamp.IsZero() {
		if err := rc.Client().Delete(
			rc.Context(),
			pod,
			client.PropagationPolicy(metav1.DeletePropagationBackground),
			client.GracePeriodSeconds(xstoreconvention.PodGracePeriodSeconds),
		); err != nil {
			return flow.Error(err, "Failed to delete the pod", "pod", exec.Step().Target)
		}
	}

	return flow.Wait("Pod's deleting, wait next try.")
}
