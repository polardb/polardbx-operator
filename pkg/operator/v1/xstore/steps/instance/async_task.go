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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func CancelHpfsAsyncTasks(ctx context.Context, client hpfs.HpfsServiceClient, pod *corev1.Pod) error {
	task, ok := pod.Annotations[xstoremeta.AnnotationAsyncTaskTransfer]
	if !ok {
		return nil
	}

	resp, err := client.CancelAsyncTask(ctx, &hpfs.CancelAsyncTaskRequest{
		Host: &hpfs.Host{NodeName: pod.Spec.NodeName},
		Task: &hpfs.AsyncTask{TraceId: task},
	})
	if err != nil {
		return err
	}
	if resp.Status.Code != hpfs.Status_OK {
		return fmt.Errorf("status not ok: " + resp.Status.Code.String())
	}

	return nil
}

func IsHpfsAsyncTaskComplete(ctx context.Context, client hpfs.HpfsServiceClient, pod *corev1.Pod) (bool, error) {
	task, ok := pod.Annotations[xstoremeta.AnnotationAsyncTaskTransfer]
	if !ok {
		return true, nil
	}

	resp, err := client.ShowAsyncTaskStatus(ctx, &hpfs.ShowAsyncTaskStatusRequest{
		Host: &hpfs.Host{NodeName: pod.Spec.NodeName},
		Task: &hpfs.AsyncTask{TraceId: task},
	})
	if err != nil {
		return false, err
	}
	if resp.Status.Code != hpfs.Status_OK {
		return false, fmt.Errorf("status not ok: " + resp.Status.Code.String())
	}

	switch resp.TaskStatus {
	case hpfs.TaskStatus_PENDING, hpfs.TaskStatus_RUNNING, hpfs.TaskStatus_CANCELING:
		return false, nil
	case hpfs.TaskStatus_SUCCESS, hpfs.TaskStatus_FAILED, hpfs.TaskStatus_CANCELED:
		return true, nil
	case hpfs.TaskStatus_UNKNOWN:
		// Consider unknown like a completed status (in order to deal with
		// cases that hpfs has lost its data).
		return true, nil
	default:
		return false, nil
	}
}

var CancelAsyncTasks = xstorev1reconcile.NewStepBinder("CancelAsyncTasks",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		hpfsClient, err := rc.GetHpfsClient()
		if err != nil {
			return flow.Error(err, "Unable to get hpfs client.")
		}

		// Cancel async tasks one-by-one.
		for _, pod := range pods {
			// By now, we only have to cancel hpfs tasks.
			err := CancelHpfsAsyncTasks(rc.Context(), hpfsClient, &pod)
			if err != nil {
				return flow.Error(err, "Unable to cancel async task", "pod", pod.Name)
			}
		}

		return flow.Pass()
	},
)

var WaitUntilAsyncTasksCanceled = xstorev1reconcile.NewStepBinder("WaitUntilAsyncTasksCanceled",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		hpfsClient, err := rc.GetHpfsClient()
		if err != nil {
			return flow.Error(err, "Unable to get hpfs client.")
		}

		// Determine if async task has completed or canceled.
		for _, pod := range pods {
			// By now, we only have to check hpfs tasks.
			completed, err := IsHpfsAsyncTaskComplete(rc.Context(), hpfsClient, &pod)
			if err != nil {
				return flow.Error(err, "Unable to determine the async task's status", "pod", pod.Name)
			}
			if !completed {
				return flow.Wait("Found async hpfs task that is still not completed or canceled.", "pod", pod.Name)
			}
		}

		return flow.Pass()
	},
)
