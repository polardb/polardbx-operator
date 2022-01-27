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

package object

import (
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/alibaba/polardbx-operator/test/framework/common"
)

var (
	poll = 2 * time.Second
)

func WaitForPodNotFoundInNamespace(c client.Client, podName string, ns string, timeout time.Duration) error {
	return common.WaitForObjectToDisappear(c, podName, ns, poll, timeout, &corev1.Pod{})
}

func WaitForPodsWithLabelsToDisappear(c client.Client, ns string, labels map[string]string, timeout time.Duration) error {
	return common.WaitForObjectsWithLabelsToDisappear(c, labels, ns, poll, timeout, &corev1.PodList{})
}

func WaitForServicesWithLabelsToDisappear(c client.Client, ns string, labels map[string]string, timeout time.Duration) error {
	return common.WaitForObjectsWithLabelsToDisappear(c, labels, ns, poll, timeout, &corev1.ServiceList{})
}

func WaitForConfigMapsWithLabelsToDisappear(c client.Client, ns string, labels map[string]string, timeout time.Duration) error {
	return common.WaitForObjectsWithLabelsToDisappear(c, labels, ns, poll, timeout, &corev1.ConfigMapList{})
}

func WaitForSecretsWithLabelsToDisappear(c client.Client, ns string, labels map[string]string, timeout time.Duration) error {
	return common.WaitForObjectsWithLabelsToDisappear(c, labels, ns, poll, timeout, &corev1.SecretList{})
}

func WaitForDeploymentsWithLabelsToDisappear(c client.Client, ns string, labels map[string]string, timeout time.Duration) error {
	return common.WaitForObjectsWithLabelsToDisappear(c, labels, ns, poll, timeout, &appsv1.DeploymentList{})
}

func WaitForJobsWithLabelsToDisappear(c client.Client, ns string, labels map[string]string, timeout time.Duration) error {
	return common.WaitForObjectsWithLabelsToDisappear(c, labels, ns, poll, timeout, &batchv1.JobList{})
}
