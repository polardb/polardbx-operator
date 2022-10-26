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
	"strconv"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/model"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/factory"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

type CreateExec struct {
	baseExec
}

func (exec *CreateExec) Execute(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	step := exec.Step()
	target := exec.Step().Target

	pod, err := rc.GetXStorePod(target)
	if client.IgnoreNotFound(err) != nil {
		return flow.Error(err, "Failed to get pod", "pod", target)
	}

	if pod != nil {
		generation, err := xstoreconvention.GetGenerationLabelValue(pod)
		if err != nil {
			return flow.Error(err, "Unable to parse generation from pod", "pod", exec.Step().Target)
		}

		if generation != step.TargetGeneration {
			panic("should not happen, must update the plan before execution")
		}

		if !k8shelper.IsPodReady(pod) {
			return flow.Wait("Pod's not ready, wait next try.", "pod", target)
		}
	} else {
		// TODO create and attach a volume here.

		xstore := rc.MustGetXStore()
		nodeSet := &polardbxv1xstore.NodeSet{
			Name:     step.NodeSet,
			Role:     polardbxv1xstore.FromNodeRoleValue(step.TargetRole),
			Replicas: int32(step.Index + 1),
			Template: exec.ec.GetNodeTemplate(step.TargetGeneration, step.NodeSet, step.Index),
		}

		pod, err = factory.NewPod(rc, xstore, nodeSet, step.Index, factory.PodFactoryOptions{
			ExtraPodFactory:     exec.ec.PodFactory,
			TemplateMergePolicy: factory.TemplateMergePolicyOverwrite,
		})
		if err != nil {
			return flow.Error(err, "Failed to construct new pod", "pod", target)
		}
		pod.Labels[xstoremeta.LabelGeneration] = strconv.FormatInt(step.TargetGeneration, 10)
		pod.Spec.NodeName = step.OriginHost

		if err := rc.SetControllerRefAndCreate(pod); err != nil {
			return flow.Error(err, "Failed to create pod", "pod", target)
		}

		return flow.Wait("Pod's creating, wait next try.")
	}

	// Add it to the paxos group.
	if !exec.ec.ContainsPaxosNode(target) {
		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.Error(err, "Failed to get leader pod")
		}
		if leaderPod == nil {
			return flow.RetryAfter(5*time.Second, "Leader pod not found")
		}

		cmd := command.NewCanonicalCommandBuilder().Consensus().AddNode(target, step.TargetRole).Build()
		buf := &bytes.Buffer{}
		if err := rc.ExecuteCommandOn(leaderPod, xstoreconvention.ContainerEngine, cmd, control.ExecOptions{
			Logger:  flow.Logger(),
			Stdin:   nil,
			Stdout:  buf,
			Stderr:  buf,
			Timeout: 5 * time.Second,
		}); err != nil {
			return flow.Error(err, "Failed to add consensus node", "pod", target)
		}

		exec.ec.AddPaxosNode(model.PaxosNodeStatus{
			PaxosNode: model.PaxosNode{
				PaxosInnerNode: model.PaxosInnerNode{
					Pod:        target,
					Role:       step.TargetRole,
					Generation: step.TargetGeneration,
					Set:        step.NodeSet,
					Index:      step.Index,
				},
				RebuildConfig: map[string]interface{}{},
			},
			Host:   pod.Spec.NodeName,
			Volume: "", // TODO
		})
	}

	exec.MarkDone()
	return flow.Pass()
}
