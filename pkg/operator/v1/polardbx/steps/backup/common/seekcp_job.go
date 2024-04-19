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

package common

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	"github.com/alibaba/polardbx-operator/pkg/util/name"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

func replaceSystemEnvs(podSpec *corev1.PodSpec, targetPod *corev1.Pod) {
	for i := range podSpec.Containers {
		c := &podSpec.Containers[i]
		for j := range c.Env {
			env := &c.Env[j]

			switch env.Name {
			case "POD_NAME":
				env.ValueFrom = nil
				env.Value = targetPod.ObjectMeta.Name
			case "POD_IP":
				env.ValueFrom = nil
				env.Value = targetPod.Status.PodIP
			case "NODE_IP":
				env.ValueFrom = nil
				env.Value = targetPod.Status.HostIP
			case "NODE_NAME":
				env.ValueFrom = nil
				env.Value = targetPod.Spec.NodeName
			}
		}
	}
}

func patchTaskConfigMapVolumeAndVolumeMounts(polardbxBackup *polardbxv1.PolarDBXBackup, podSpec *corev1.PodSpec) {
	podSpec.Volumes = k8shelper.PatchVolumes(podSpec.Volumes, []corev1.Volume{
		{
			Name: "seekcp",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: name.PolarDBXBackupStableName(polardbxBackup, "seekcp"),
					},
				},
			},
		},
	})

	for i := range podSpec.Containers {
		c := &podSpec.Containers[i]
		c.VolumeMounts = k8shelper.PatchVolumeMounts(c.VolumeMounts, []corev1.VolumeMount{
			{
				Name:      "seekcp",
				ReadOnly:  true,
				MountPath: "/seekcp",
			},
		})
	}
}

func newSeekCpJob(pxcBackup *polardbxv1.PolarDBXBackup, targetPod *corev1.Pod) (*batchv1.Job, error) {
	podSpec := targetPod.Spec.DeepCopy()
	podSpec.InitContainers = nil
	podSpec.RestartPolicy = corev1.RestartPolicyNever
	podSpec.HostNetwork = false

	podSpec.Containers = []corev1.Container{
		*k8shelper.GetContainerFromPodSpec(podSpec, "engine"),
	}
	podSpec.Containers[0].Name = "seekcpjob"

	podSpec.Containers[0].Command = command.NewCanonicalCommandBuilder().Seekcp().
		StartSeekcp("/seekcp/seekcp").Build()
	podSpec.Containers[0].Resources.Limits = nil
	podSpec.Containers[0].Resources.Requests = nil
	podSpec.Containers[0].Ports = nil
	if podSpec.Containers[0].Lifecycle != nil {
		podSpec.Containers[0].Lifecycle.PreStop = nil
	}

	// Replace system envs
	replaceSystemEnvs(podSpec, targetPod)
	patchTaskConfigMapVolumeAndVolumeMounts(pxcBackup, podSpec)

	jobName := name.NewSplicedName(
		name.WithTokens("seekcp", "job", pxcBackup.Name),
		name.WithPrefix("seekcp-job"),
	)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: pxcBackup.Namespace,
			Labels: map[string]string{
				meta.SeekCpJobLabelBackupName: pxcBackup.Name,
				meta.SeekCpJobLabelPXCName:    pxcBackup.Spec.Cluster.Name,
				meta.SeekCpJobLabelPodName:    targetPod.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: pointer.Int32(0),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						meta.SeekCpJobLabelBackupName: pxcBackup.Name,
						meta.SeekCpJobLabelPXCName:    pxcBackup.Spec.Cluster.Name,
						meta.SeekCpJobLabelPodName:    targetPod.Name,
					},
				},
				Spec: *podSpec,
			},
		},
	}
	return job, nil
}
