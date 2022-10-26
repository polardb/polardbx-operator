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

package backup

import (
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	"strconv"
)

func newBinlogBackupJob(xstoreBackup *xstorev1.XStoreBackup, targetPod *corev1.Pod, jobName string, isGMS bool) (*batchv1.Job, error) {
	podSpec := targetPod.Spec.DeepCopy()
	podSpec.InitContainers = nil
	podSpec.RestartPolicy = corev1.RestartPolicyNever
	podSpec.HostNetwork = false

	podSpec.Containers = []corev1.Container{
		*k8shelper.GetContainerFromPodSpec(podSpec, "engine"),
	}
	podSpec.Containers[0].Name = "binlogbackupjob"
	xstoreName := xstoreBackup.Spec.XStore.Name

	CommitIndex := xstoreBackup.Status.CommitIndex
	gmsLabel := "false"
	if isGMS {
		gmsLabel = "true"
	}
	podSpec.Containers[0].Command = command.NewCanonicalCommandBuilder().BinlogBackup().
		StartBinlogBackup("/backup/backup", strconv.FormatInt(CommitIndex, 10), xstoreName, gmsLabel).Build()
	podSpec.Containers[0].Resources.Limits = nil
	podSpec.Containers[0].Resources.Requests = nil
	podSpec.Containers[0].Ports = nil

	// Replace system envs
	replaceSystemEnvs(podSpec, targetPod)
	patchTaskConfigMapVolumeAndVolumeMounts(xstoreBackup, podSpec)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: xstoreBackup.Namespace,
			Labels: map[string]string{
				xstoremeta.JobLabelTargetPod:           targetPod.Name,
				xstoremeta.JobLabelTargetNodeName:      targetPod.Spec.NodeName,
				xstoremeta.LabelXStoreBinlogBackupName: xstoreBackup.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: pointer.Int32(0),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						xstoremeta.JobLabelTargetPod:           targetPod.Name,
						xstoremeta.JobLabelTargetNodeName:      targetPod.Spec.NodeName,
						xstoremeta.LabelXStoreBinlogBackupName: xstoreBackup.Name,
					},
				},
				Spec: *podSpec,
			},
		},
	}
	return job, nil
}
