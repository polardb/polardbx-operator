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
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/factory"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

type UpdateExec struct {
	baseExec
}

func (exec *UpdateExec) Execute(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
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

		if generation == step.TargetGeneration {
			if !k8shelper.IsPodReady(pod) {
				return flow.Wait("Pod's not ready, wait next try.")
			}
			exec.MarkDone()
			return flow.Pass()
		}

		// Delete it first.
		if pod.DeletionTimestamp.IsZero() {
			if err := rc.Client().Delete(
				rc.Context(),
				pod,
				client.PropagationPolicy(metav1.DeletePropagationBackground),
			); err != nil {
				return flow.Error(err, "Unable to delete the pod", "pod", target)
			}
		}
		return flow.Wait("Pod's deleting, wait next try.")
	} else {
		// Create a new pod on the last host.
		xstore := rc.MustGetXStore()
		nodeSet := &polardbxv1xstore.NodeSet{
			Name:     step.NodeSet,
			Role:     polardbxv1xstore.FromNodeRoleValue(step.TargetRole),
			Replicas: int32(step.Index + 1),
			Template: exec.ec.GetNodeTemplate(step.TargetGeneration, step.NodeSet, step.Index),
		}

		// TODO get the host and volume from context and create one.

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
}
