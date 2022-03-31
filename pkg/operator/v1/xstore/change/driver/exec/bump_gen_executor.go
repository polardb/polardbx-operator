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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

type BumpGenExec struct {
	baseExec
}

func (exec *BumpGenExec) Execute(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	step := exec.Step()

	pod, err := rc.GetXStorePod(exec.Step().Target)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return flow.RetryAfter(time.Second, "Pod not found", "pod", exec.Step().Target)
		} else {
			return flow.Error(err, "Failed to get pod", "pod", exec.Step().Target)
		}
	}

	generation, err := xstoreconvention.GetGenerationLabelValue(pod)
	if err != nil {
		return flow.Error(err, "Unable to parse generation from pod", "pod", exec.Step().Target)
	}

	if generation == step.TargetGeneration {
		exec.MarkDone()
		return flow.Pass()
	}

	updatedPod := pod.DeepCopy()
	updatedPod.Labels[xstoremeta.LabelGeneration] = strconv.FormatInt(step.TargetGeneration, 10)
	if err := rc.Client().Patch(rc.Context(), updatedPod, client.MergeFrom(pod)); err != nil {
		return flow.Error(err, "Unable to patch pod.", "pod", exec.Step().Target)
	}

	return flow.Wait("Pod's label updated!")
}
