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

import (
	"sort"

	corev1 "k8s.io/api/core/v1"
)

func PatchVolumes(origin []corev1.Volume, patches ...[]corev1.Volume) []corev1.Volume {
	for _, patch := range patches {
		origin = append(origin, patch...)
	}

	// Sort to provide a stable slice.
	sort.Slice(origin, func(i, j int) bool {
		return origin[i].Name < origin[j].Name
	})

	return origin
}

func PatchVolumeMounts(origin []corev1.VolumeMount, patches ...[]corev1.VolumeMount) []corev1.VolumeMount {
	for _, patch := range patches {
		origin = append(origin, patch...)
	}

	// Sort to provide a stable slice.
	sort.Slice(origin, func(i, j int) bool {
		return origin[i].Name < origin[j].Name
	})

	return origin
}
