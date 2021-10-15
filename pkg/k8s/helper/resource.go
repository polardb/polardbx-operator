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

package helper

import corev1 "k8s.io/api/core/v1"

func IsResourceQoSGuaranteed(resourceRequirements corev1.ResourceRequirements) bool {
	if resourceRequirements.Limits != nil && resourceRequirements.Requests == nil {
		_, ok := resourceRequirements.Limits[corev1.ResourceCPU]
		if !ok {
			return false
		}
		_, ok = resourceRequirements.Limits[corev1.ResourceMemory]
		if !ok {
			return false
		}
		return true
	}
	return false
}

func IsContainerQoSGuaranteed(c *corev1.Container) bool {
	return IsResourceQoSGuaranteed(c.Resources)
}
