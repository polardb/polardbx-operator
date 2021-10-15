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
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1common "github.com/alibaba/polardbx-operator/api/v1/common"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func SyncBlkioCgroupValuesViaHpfs(ctx context.Context, hpfsClient hpfs.HpfsServiceClient, pod *corev1.Pod,
	vol *polardbxv1xstore.HostPathVolume, resources *polardbxv1common.ExtendedResourceRequirements) error {
	resp, err := hpfsClient.ControlCgroupsBlkio(ctx, &hpfs.ControlCgroupsBlkioRequest{
		Host:   &hpfs.Host{NodeName: pod.Spec.NodeName},
		PodUid: string(pod.UID),
		Controls: []*hpfs.BlkioCtrl{
			{
				Key: hpfs.BlkioKey_IOPS_READ,
				Device: &hpfs.BlkioCtrl_Path{
					Path: vol.HostPath,
				},
				Value: polardbxv1common.GetResourceReadIOPSValue(resources.LimitsIO),
			},
			{
				Key: hpfs.BlkioKey_IOPS_WRITE,
				Device: &hpfs.BlkioCtrl_Path{
					Path: vol.HostPath,
				},
				Value: polardbxv1common.GetResourceWriteIOPSValue(resources.LimitsIO),
			},
			{
				Key: hpfs.BlkioKey_BPS_READ,
				Device: &hpfs.BlkioCtrl_Path{
					Path: vol.HostPath,
				},
				Value: polardbxv1common.GetResourceReadBPSValue(resources.LimitsIO),
			},
			{
				Key: hpfs.BlkioKey_BPS_WRITE,
				Device: &hpfs.BlkioCtrl_Path{
					Path: vol.HostPath,
				},
				Value: polardbxv1common.GetResourceWriteBPSValue(resources.LimitsIO),
			},
		},
	})

	if err != nil {
		return err
	}

	if resp.Status.Code != hpfs.Status_OK {
		return errors.New("status not ok: " + resp.Status.Code.String())
	}

	return nil
}

var SyncBlkioCgroupResourceLimits = xstorev1reconcile.NewStepBinder("SyncBlkioCgroupResourceLimits",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		hpfsClient, err := rc.GetHpfsClient()
		if err != nil {
			return flow.Error(err, "Unable to get hpfs client")
		}

		// Total used time should be no longer than 10 seconds.
		ctx, cancel := context.WithTimeout(rc.Context(), 10*time.Second)
		defer cancel()

		xstore := rc.MustGetXStore()
		topology := xstore.Spec.Topology
		nodeSets := make(map[string]*polardbxv1xstore.NodeSet)
		for i := range topology.NodeSets {
			ns := &topology.NodeSets[i]
			nodeSets[ns.Name] = ns
		}

		volumes := xstore.Status.BoundVolumes
		for _, pod := range pods {
			ns := nodeSets[pod.Labels[xstoremeta.LabelNodeSet]]
			if ns == nil {
				continue
			}

			resources := topology.Template.Spec.Resources
			if ns.Template != nil && ns.Template.Spec.Resources != nil {
				resources = ns.Template.Spec.Resources
			}

			if resources == nil || resources.LimitsIO == nil {
				continue
			}

			blkioVal, ok := polardbxv1common.ResourceBlkioValueStr(resources.LimitsIO)
			if !ok {
				continue
			}

			// Skip those already configured.
			annotationVal := pod.ObjectMeta.Annotations[xstoremeta.AnnotationBlkioResourceLimit]
			if annotationVal == blkioVal {
				continue
			}

			// Sync via hpfs.
			err := SyncBlkioCgroupValuesViaHpfs(ctx, hpfsClient, &pod, volumes[pod.Name], resources)
			if err != nil {
				return flow.Error(err, "Failed to sync blkio cgroup values.", "host", pod.Spec.NodeName,
					"pod", pod.Name, "blkio", blkioVal)
			}

			// update pod
			metav1.SetMetaDataAnnotation(&pod.ObjectMeta, xstoremeta.AnnotationBlkioResourceLimit, blkioVal)
			if err := rc.Client().Update(ctx, &pod); err != nil {
				return flow.Error(err, "Unable to update pod.", "pod", pod.Name)
			}
		}

		return flow.Continue("Succeeds to sync cgroup blkio values.")
	},
)
