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

package instance

import (
	"encoding/json"

	"gomodules.xyz/jsonpatch/v2"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/context"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/model"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func LoadExecutionContext(rc *xstorev1reconcile.Context) (*context.ExecutionContext, error) {
	taskCm, err := rc.GetXStoreConfigMap(convention.ConfigMapTypeTask)
	if err != nil {
		return nil, err
	}
	ecVal, ok := taskCm.Data["exec"]
	if !ok {
		return nil, nil
	}
	var ec context.ExecutionContext
	err = json.Unmarshal([]byte(ecVal), &ec)
	if err != nil {
		return nil, err
	}
	return &ec, nil
}

func NewExecutionContext(rc *xstorev1reconcile.Context, xstore *polardbxv1.XStore, selfHeal bool) (*context.ExecutionContext, error) {
	volumes := xstore.Status.BoundVolumes
	paxosVolumes := make(map[string]model.PaxosVolume)
	for _, vol := range volumes {
		// FIXME, role is determined.
		paxosVolumes[vol.Pod] = model.PaxosVolume{
			Name:     vol.Pod,
			Host:     vol.Host,
			HostPath: vol.HostPath,
			Role:     "",
		}
	}

	// Running
	pods, err := rc.GetXStorePods()
	if err != nil {
		return nil, err
	}
	running, err := model.FromPods(k8shelper.FilterPodsBy(pods, k8shelper.IsPodRunning))
	if err != nil {
		return nil, err
	}

	generation := xstore.Generation
	if selfHeal {
		generation = xstore.Status.ObservedGeneration
	}
	return &context.ExecutionContext{
		Generation: generation,
		Running:    model.ToMap(running),
		// FIXME.
		Tracking:   model.ToMap(running),
		Expected:   nil,
		Volumes:    paxosVolumes,
		Plan:       nil,
		StepIndex:  0,
		PodFactory: nil,
	}, nil
}

func isExecutionContextChanged(old, now []byte) bool {
	if old == nil {
		return now != nil
	}
	patches, err := jsonpatch.CreatePatch(old, now)
	if err != nil {
		panic(err)
	}
	return len(patches) > 0
}

func TrackAndLazyUpdateExecuteContext(current *context.ExecutionContext) control.BindFunc {
	// Capture the old value immediately.
	old := []byte(nil)
	if current != nil {
		var err error
		old, err = json.Marshal(current)
		if err != nil {
			panic(err)
		}
	}

	return xstorev1reconcile.NewStepBinder("TrackAndLazyUpdateExecuteContext", func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		// Compare it with now execution context.
		now, err := json.MarshalIndent(current, "", "  ")
		if err != nil {
			panic(err)
		}

		if isExecutionContextChanged(old, now) {
			taskCm, err := rc.GetXStoreConfigMap(convention.ConfigMapTypeTask)
			if err != nil {
				return flow.Error(err, "Unable to get task config map.")
			}

			if taskCm.Data == nil {
				taskCm.Data = make(map[string]string)
			}
			taskCm.Data["exec"] = string(now)
			if err := rc.Client().Update(rc.Context(), taskCm); err != nil {
				return flow.Error(err, "Unable to update tass config map.")
			}
			return flow.Continue("Updated!")
		}

		return flow.Continue("Not changed.")
	})
}

var DeleteExecutionContext = xstorev1reconcile.NewStepBinder("DeleteExecutionContext",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		taskCm, err := rc.GetXStoreConfigMap(convention.ConfigMapTypeTask)
		if err != nil {
			return flow.Error(err, "Unable to get task config map.")
		}

		_, ok := taskCm.Data["exec"]
		if !ok {
			return flow.Pass()
		}

		delete(taskCm.Data, "exec")
		if err := rc.Client().Update(rc.Context(), taskCm); err != nil {
			return flow.Error(err, "Unable to update tass config map.")
		}
		return flow.Continue("Deleted!")
	},
)
