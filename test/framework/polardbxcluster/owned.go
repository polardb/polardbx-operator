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

package polardbxcluster

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
)

func ListPodsShouldBeOwnedByPolarDBXCluster(ctx context.Context, c client.Client, polardbxcluster *polardbxv1.PolarDBXCluster, additionalLabels map[string]string) ([]corev1.Pod, error) {
	var podList corev1.PodList
	err := c.List(ctx, &podList, client.InNamespace(polardbxcluster.Namespace),
		client.MatchingLabels(
			k8shelper.PatchLabels(map[string]string{
				polardbxmeta.LabelName: polardbxcluster.Name,
			}, additionalLabels),
		))
	if err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func ListServicesShouldBeOwnedByPolarDBXCluster(ctx context.Context, c client.Client, polardbxcluster *polardbxv1.PolarDBXCluster, additionalLabels map[string]string) ([]corev1.Service, error) {
	var srvList corev1.ServiceList
	err := c.List(ctx, &srvList, client.InNamespace(polardbxcluster.Namespace),
		client.MatchingLabels(
			k8shelper.PatchLabels(map[string]string{
				polardbxmeta.LabelName: polardbxcluster.Name,
			}, additionalLabels),
		))
	if err != nil {
		return nil, err
	}
	return srvList.Items, nil
}
