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
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	polardbxreconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
)

func RemoveAnnotation(key string) control.BindFunc {
	return polardbxreconcile.NewStepBinder("RemoveAnnotation_"+key, func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if _, ok := polardbx.Annotations[key]; ok {
			delete(polardbx.Annotations, key)
			rc.MarkPolarDBXChanged()
		}
		return flow.Pass()
	})
}

var TrySetRunMode = polardbxreconcile.NewStepBinder("TrySetRunMode",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if !rc.Config().Cluster().EnableRunModeCheck() {
			return flow.Pass()
		}
		if polardbx.Annotations == nil {
			polardbx.SetAnnotations(map[string]string{})
		}
		runmode, ok := polardbx.Annotations["runmode"]
		if !ok {
			runmode = "none"
		}
		var podList v1.PodList
		err := rc.Client().List(rc.Context(), &podList, client.InNamespace(polardbx.Namespace), client.MatchingLabels{
			polardbxmeta.LabelName: polardbx.Name,
		})
		if err != nil {
			return flow.RetryErr(err, "failed to get podlist")
		}

		for _, pod := range podList.Items {
			annotations := pod.GetAnnotations()
			if annotations == nil {
				annotations = map[string]string{}
			}
			mode, ok := annotations["runmode"]
			if !ok || mode != runmode {
				annotations["runmode"] = runmode
				pod.SetAnnotations(annotations)
				err := rc.Client().Update(rc.Context(), &pod)
				if err != nil {
					return flow.RetryErr(err, "failed to update pod runmode annotation", "PodName", pod.GetName())
				}
			}
		}
		return flow.Pass()
	},
)
