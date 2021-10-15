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

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
)

func SystemVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: "etclocaltime",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/etc/localtime",
					Type: k8shelper.HostPathTypePtr(corev1.HostPathFile),
				},
			},
		},
		{
			Name: "zoneinfo",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/usr/share/zoneinfo",
					Type: k8shelper.HostPathTypePtr(corev1.HostPathDirectory),
				},
			},
		},
		{
			Name: "sysfscgroup",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/sys/fs/cgroup",
					Type: k8shelper.HostPathTypePtr(corev1.HostPathDirectory),
				},
			},
		},
		{
			Name: "podinfo",
			VolumeSource: corev1.VolumeSource{
				DownwardAPI: &corev1.DownwardAPIVolumeSource{
					Items: []corev1.DownwardAPIVolumeFile{
						{
							Path: "labels",
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.labels",
							},
						},
						{
							Path: "annotations",
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.annotations",
							},
						},
						{
							Path: "runmode",
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.annotations['runmode']",
							},
						},
						{
							Path: "name",
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.name",
							},
						},
						{
							Path: "namespace",
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.namespace",
							},
						},
					},
				},
			},
		},
	}
}

func SystemVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:             "etclocaltime",
			MountPath:        "/etc/localtime",
			MountPropagation: k8shelper.MountPropagationModePtr(corev1.MountPropagationNone),
			ReadOnly:         true,
		},
		{
			Name:             "zoneinfo",
			MountPath:        "/usr/share/zoneinfo",
			MountPropagation: k8shelper.MountPropagationModePtr(corev1.MountPropagationNone),
			ReadOnly:         true,
		},
		{
			Name:             "sysfscgroup",
			MountPath:        "/sys/fs/cgroup",
			MountPropagation: k8shelper.MountPropagationModePtr(corev1.MountPropagationNone),
		},
		{
			Name:      "podinfo",
			MountPath: "/etc/podinfo",
		},
	}
}

func ConfigMapVolumes(xstore *polardbxv1.XStore) []corev1.Volume {
	return []corev1.Volume{
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: convention.NewConfigMapName(xstore, convention.ConfigMapTypeConfig),
					},
				},
			},
		},
		{
			Name: "shared",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: convention.NewConfigMapName(xstore, convention.ConfigMapTypeShared),
					},
				},
			},
		},
	}
}

func ConfigMapVolumeMounts(xstore *polardbxv1.XStore) []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      "config",
			ReadOnly:  true,
			MountPath: "/data/config",
		},
		{
			Name:      "shared",
			ReadOnly:  true,
			MountPath: "/data/shared",
		},
	}
}
