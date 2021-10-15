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
	"path"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func podHostPathVolumeBaseDir(xstore *polardbxv1.XStore, config config.Config) string {
	return path.Join(config.Store().HostPathDataVolumeRoot(), xstore.Namespace)
}

// preparePodVolumeBindings returns a pod and random host path volume path map.
func preparePodVolumeBindings(xstore *polardbxv1.XStore, config config.Config) map[string]string {
	topology := xstore.Spec.Topology

	podVolumeBindings := make(map[string]string)
	for _, nodeSet := range topology.NodeSets {
		for i := 0; i < int(nodeSet.Replicas); i++ {
			podName := convention.NewPodName(xstore, &nodeSet, i)
			podVolumeBindings[podName] = path.Join(podHostPathVolumeBaseDir(xstore, config), podName)
		}
	}
	return podVolumeBindings
}

var PrepareHostPathVolumes = xstorev1reconcile.NewStepBinder("PrepareHostPathVolumes",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		podVolumes := preparePodVolumeBindings(xstore, rc.Config())
		volumes := make(map[string]*polardbxv1xstore.HostPathVolume)
		for pod, vPath := range podVolumes {
			volumes[pod] = &polardbxv1xstore.HostPathVolume{
				Pod:      pod,
				HostPath: vPath,
				Type:     corev1.HostPathDirectory,
			}
		}
		xstore.Status.BoundVolumes = volumes

		return flow.Continue("Host path volumes prepared.")
	},
)

var BindHostPathVolumesToHost = xstorev1reconcile.NewStepBinder("BindHostPathVolumesToHost",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		binding := xstore.Status.BoundVolumes
		for _, pod := range pods {
			if len(pod.Spec.NodeName) == 0 {
				return flow.Wait("Some pod is still not scheduled.", "pod", pod.Name)
			}
			binding[pod.Name].Host = pod.Spec.NodeName
		}

		return flow.Pass()
	},
)

func DeleteHostPathVolume(ctx context.Context, hpfsClient hpfs.HpfsServiceClient, vol *polardbxv1xstore.HostPathVolume) error {
	switch vol.Type {
	case corev1.HostPathFile:
		resp, err := hpfsClient.RemoveFile(ctx, &hpfs.RemoveFileRequest{
			Host:    &hpfs.Host{NodeName: vol.Host},
			Options: &hpfs.RemoveOptions{Recursive: true, IgnoreIfNotExists: true},
			Path:    vol.HostPath,
		})
		if err != nil {
			return err
		}
		if resp.Status.Code != hpfs.Status_OK {
			return errors.New("status not ok: " + resp.Status.Code.String())
		}
	case corev1.HostPathDirectory:
		resp, err := hpfsClient.RemoveDirectory(ctx, &hpfs.RemoveDirectoryRequest{
			Host:    &hpfs.Host{NodeName: vol.Host},
			Options: &hpfs.RemoveOptions{Recursive: true, IgnoreIfNotExists: true},
			Path:    vol.HostPath,
		})
		if err != nil {
			return err
		}
		if resp.Status.Code != hpfs.Status_OK {
			return errors.New("status not ok: " + resp.Status.Code.String())
		}
	default: // Unrecognized, do nothing
	}
	return nil
}

var DeleteHostPathVolumes = xstorev1reconcile.NewStepBinder("DeleteHostPathVolumes",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		// Skip if volumes already deleted.
		volumes := xstore.Status.BoundVolumes
		if volumes == nil {
			return flow.Pass()
		}

		// Try our best to delete the volumes.
		nodes, err := rc.GetNodes()
		if err != nil {
			return flow.Error(err, "Unable to get nodes.")
		}
		observedNodes := k8shelper.ToObjectNameSet(nodes)

		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}
		podMap := k8shelper.BuildPodMap(pods, func(pod *corev1.Pod) string {
			return pod.Name
		})

		// Delete one-by-one.
		hpfsClient, err := rc.GetHpfsClient()
		if err != nil {
			return flow.Error(err, "Unable to get hpfs client.")
		}
		for podName, vol := range volumes {
			// Set the host to pod's node name if not bound.
			if len(vol.Host) == 0 {
				pod := podMap[podName]
				if pod != nil {
					vol.Host = pod.Spec.NodeName
				}
			}

			// Branch volume isn't bound on some observed node, ignore.
			if _, ok := observedNodes[vol.Host]; !ok {
				continue
			}

			err := DeleteHostPathVolume(rc.Context(), hpfsClient, vol)
			if err != nil {
				return flow.Error(err, "Unable to remove host path volume.", "vol.pod", podName,
					"vol.host", vol.Host, "vol.type", vol.Type, "vol.path", vol.HostPath)
			}
		}

		// Mark volumes as deleted.
		xstore.Status.BoundVolumes = nil

		return flow.Continue("Volumes are deleted.")
	})

func UpdateHostPathVolumeSizesTemplate(d time.Duration) control.BindFunc {
	return xstorev1reconcile.NewStepBinder("UpdateHostPathVolumeSizesPer"+d.String(),
		func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			xstore := rc.MustGetXStore()

			now := metav1.Now()
			if xstore.Status.LastVolumeSizeUpdateTime != nil &&
				xstore.Status.LastVolumeSizeUpdateTime.Add(d).After(now.Time) {
				return flow.Continue("Not time to update sizes, skip.")
			}

			hpfsClient, err := rc.GetHpfsClient()
			if err != nil {
				return flow.Error(err, "Unable to get hpfs client.")
			}

			// Total used time should be no longer than 10 seconds.
			ctx, cancel := context.WithTimeout(rc.Context(), 10*time.Second)
			defer cancel()

			volumes := xstore.Status.BoundVolumes
			for pod, vol := range volumes {
				if vol == nil || len(vol.Host) == 0 || len(vol.HostPath) == 0 {
					continue
				}

				resp, err := hpfsClient.ShowDiskUsage(ctx, &hpfs.ShowDiskUsageRequest{
					Host: &hpfs.Host{NodeName: vol.Host},
					Path: vol.HostPath,
				})
				if err != nil {
					flow.Logger().Error(err, "Unable to get disk usage.", "vol.pod", pod, "vol.host",
						vol.Host, "vol.path", vol.HostPath)
				} else if resp.Status.Code != hpfs.Status_OK {
					flow.Logger().Error(errors.New("status not ok: "+resp.Status.Code.String()), "Failed to get disk usage.",
						"vol.pod", pod, "vol.host", vol.Host, "vol.path", vol.HostPath)
				} else {
					vol.Size = int64(resp.Size)
				}
			}

			xstore.Status.LastVolumeSizeUpdateTime = &now

			return flow.Continue("Succeeds to update sizes for host path volumes.")
		},
	)
}
