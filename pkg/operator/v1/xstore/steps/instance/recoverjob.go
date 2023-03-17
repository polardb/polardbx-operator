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
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/util/name"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

func newRecoverDataJob(xstore *xstorev1.XStore, targetPod *corev1.Pod, secret string) *batchv1.Job {
	podSpec := targetPod.Spec.DeepCopy()
	podSpec.InitContainers = nil
	podSpec.RestartPolicy = corev1.RestartPolicyNever
	podSpec.HostNetwork = false

	// Remove containers except engine
	podSpec.Containers = []corev1.Container{
		*k8shelper.GetContainerFromPodSpec(podSpec, "engine"),
	}
	podSpec.Containers[0].Name = "recoverjob"

	podSpec.Containers[0].Command = command.NewCanonicalCommandBuilder().Recover().StartRecover("/restore/restore", targetPod.Name, secret).Build()
	podSpec.Containers[0].Resources.Limits = nil
	podSpec.Containers[0].Resources.Requests = nil
	podSpec.Containers[0].Ports = nil

	podSpec.Containers[0].LivenessProbe = nil
	podSpec.Containers[0].ReadinessProbe = nil
	podSpec.Containers[0].StartupProbe = nil

	// Replace system envs.
	replaceSystemEnvs(podSpec, targetPod)
	patchTaskConfigMapVolumeAndVolumeMounts(xstore, podSpec)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name.StableName(xstore, name.GetStableNameSuffix(xstore, targetPod.Name)+"-recover"),
			Namespace: xstore.Namespace,
			Labels: map[string]string{
				xstoremeta.LabelName:              xstore.Name,
				xstoremeta.LabelRand:              xstore.Status.Rand,
				xstoremeta.JobLabelTargetPod:      targetPod.Name,
				xstoremeta.JobLabelTargetNodeName: targetPod.Spec.NodeName,
			},
		},
		Spec: batchv1.JobSpec{
			//TTLSecondsAfterFinished: pointer.Int32(100),
			BackoffLimit: pointer.Int32(0),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						xstoremeta.LabelName: xstore.Name,
						xstoremeta.LabelRand: xstore.Status.Rand,
					},
				},
				Spec: *podSpec,
			},
		},
	}
}
