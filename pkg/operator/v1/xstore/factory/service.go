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

package factory

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
)

func NewHeadlessService(xstore *polardbxv1.XStore, podName string) *corev1.Service {
	serviceLabels := k8shelper.DeepCopyStrMap(xstore.Spec.ServiceLabels)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewHeadlessServiceName(podName),
			Namespace: xstore.Namespace,
			Labels: k8shelper.PatchLabels(
				serviceLabels,
				convention.ConstLabels(xstore),
				map[string]string{
					xstoremeta.LabelServiceType: xstoremeta.ServiceTypeHeadless,
					xstoremeta.LabelPod:         podName,
				},
			),
			Annotations: map[string]string{},
		},
		Spec: corev1.ServiceSpec{
			// Selects to the pod.
			Selector: k8shelper.PatchLabels(
				convention.ConstLabels(xstore),
				map[string]string{
					xstoremeta.LabelPod: podName,
				},
			),
			// Headless by setting .spec.clusterIP to None explicitly.
			ClusterIP: corev1.ClusterIPNone,
			// Must publish not ready addresses.
			PublishNotReadyAddresses: true,
		},
	}
}
