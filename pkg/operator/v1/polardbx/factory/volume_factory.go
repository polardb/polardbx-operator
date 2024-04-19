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
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
)

type VolumeFactory interface {
	NewSystemVolumes() []corev1.Volume
	NewSystemVolumeMounts() []corev1.VolumeMount
	NewVolumesForCN() []corev1.Volume
	NewVolumeMountsForCNEngine() []corev1.VolumeMount
	NewVolumeMountsForCNExporter() []corev1.VolumeMount
	NewVolumesForCDC() []corev1.Volume
	NewVolumeMountsForCDCEngine() []corev1.VolumeMount
	NewVolumesForColumnar() []corev1.Volume
	NewVolumeMountsForColumnarEngine() []corev1.VolumeMount
}

type volumeFactory struct {
	rc       *polardbxv1reconcile.Context
	polardbx *polardbxv1.PolarDBXCluster
}

func hostPathTypePtr(t corev1.HostPathType) *corev1.HostPathType {
	return &t
}

func mountPropagationModePtr(m corev1.MountPropagationMode) *corev1.MountPropagationMode {
	return &m
}

func (v *volumeFactory) NewSystemVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name:         "etclocaltime",
			VolumeSource: v.newHostPathVolumeSource("/etc/localtime", corev1.HostPathFile),
		},
		{
			Name:         "zoneinfo",
			VolumeSource: v.newHostPathVolumeSource("/usr/share/zoneinfo", corev1.HostPathDirectory),
		},
		{
			Name:         "sysfscgroup",
			VolumeSource: v.newHostPathVolumeSource("/sys/fs/cgroup", corev1.HostPathDirectory),
		},
		{
			Name:         "tmp",
			VolumeSource: v.newEmptyDirVolumeSource(),
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

func (v *volumeFactory) NewSystemVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:             "etclocaltime",
			MountPath:        "/etc/localtime",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationNone),
			ReadOnly:         true,
		},
		{
			Name:             "zoneinfo",
			MountPath:        "/usr/share/zoneinfo",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationNone),
			ReadOnly:         true,
		},
		{
			Name:             "sysfscgroup",
			MountPath:        "/sys/fs/cgroup",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationNone),
		},
		{
			Name:             "tmp",
			MountPath:        "/tmp",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		},
		{
			Name:      "podinfo",
			MountPath: "/etc/podinfo",
		},
	}
}

func (v *volumeFactory) newEmptyDirVolumeSource() corev1.VolumeSource {
	return corev1.VolumeSource{
		EmptyDir: &corev1.EmptyDirVolumeSource{},
	}
}

func (v *volumeFactory) newNFS(path string, server string) corev1.VolumeSource {
	return corev1.VolumeSource{
		NFS: &corev1.NFSVolumeSource{
			Path:   path,
			Server: server,
		},
	}
}

func (v *volumeFactory) newConfigMapVolumeSource(name string, items []corev1.KeyToPath) corev1.VolumeSource {
	return corev1.VolumeSource{
		ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: name,
			},
			Items: items,
		},
	}
}

func (v *volumeFactory) newSecretVolumeSource(name string, items []corev1.KeyToPath) corev1.VolumeSource {
	return corev1.VolumeSource{
		Secret: &corev1.SecretVolumeSource{
			SecretName: name,
			Items:      items,
		},
	}
}

func (v *volumeFactory) newHostPathVolumeSource(path string, hostPathType corev1.HostPathType) corev1.VolumeSource {
	return corev1.VolumeSource{
		HostPath: &corev1.HostPathVolumeSource{
			Path: path,
			Type: &hostPathType,
		},
	}
}

func (v *volumeFactory) NewVolumesForCN() []corev1.Volume {
	systemVols := v.NewSystemVolumes()

	configCmName := convention.NewConfigMapName(v.polardbx, convention.ConfigMapTypeConfig)
	configSecurityName := convention.NewSecretName(v.polardbx, convention.SecretTypeSecurity)
	volumes := []corev1.Volume{
		{
			Name:         "polardbx-log",
			VolumeSource: v.newEmptyDirVolumeSource(),
		},
		{
			Name:         "polardbx-spill",
			VolumeSource: v.newEmptyDirVolumeSource(),
		},
		{
			Name:         "polardbx-config",
			VolumeSource: v.newConfigMapVolumeSource(configCmName, nil),
		},
		{
			Name:         "shared",
			VolumeSource: v.newHostPathVolumeSource("/data/polardbx/__shared", corev1.HostPathDirectoryOrCreate),
		},
		{
			Name:         "polardbx-security",
			VolumeSource: v.newSecretVolumeSource(configSecurityName, nil),
		},
	}

	nfsConfig := v.rc.Config().Nfs()
	if nfsConfig.Server() != "" {
		volumes = append(volumes, corev1.Volume{
			Name:         "polardbx-nfs",
			VolumeSource: v.newNFS(nfsConfig.Path(), nfsConfig.Server()),
		})
	}

	return append(systemVols, volumes...)
}

func (v *volumeFactory) NewVolumeMountsForCNEngine() []corev1.VolumeMount {
	systemVolMounts := v.NewSystemVolumeMounts()
	volumeMounts := []corev1.VolumeMount{
		{
			Name:             "polardbx-log",
			MountPath:        "/home/admin/drds-server/logs",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		},
		{
			Name:             "polardbx-spill",
			MountPath:        "/home/admin/drds-server/spill",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		},
		{
			Name:             "polardbx-config",
			MountPath:        "/home/admin/drds-server/env/config.properties",
			ReadOnly:         true,
			SubPath:          "config.properties",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		},
		{
			Name:             "shared",
			MountPath:        "/shared",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		},
		{
			Name:             "polardbx-security",
			MountPath:        "/home/admin/drds-server/security",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		},
	}

	nfsConfig := v.rc.Config().Nfs()
	if nfsConfig.Server() != "" {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:             "polardbx-nfs",
			MountPath:        "/home/admin/polardbx-external-disk",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		})
	}

	return append(systemVolMounts, volumeMounts...)
}

func (v *volumeFactory) NewVolumeMountsForCNExporter() []corev1.VolumeMount {
	return v.NewSystemVolumeMounts()
}

func (v *volumeFactory) NewVolumesForCDC() []corev1.Volume {
	systemVols := v.NewSystemVolumes()
	volumes := []corev1.Volume{
		{
			Name:         "binlog",
			VolumeSource: v.newEmptyDirVolumeSource(),
		},
		{
			Name:         "log",
			VolumeSource: v.newEmptyDirVolumeSource(),
		},
	}
	return append(systemVols, volumes...)
}

func (v *volumeFactory) NewVolumeMountsForCDCEngine() []corev1.VolumeMount {
	systemVolMounts := v.NewSystemVolumeMounts()
	volumeMounts := []corev1.VolumeMount{
		{
			Name:             "binlog",
			MountPath:        "/home/admin/binlog",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		},
		{
			Name:             "log",
			MountPath:        "/home/admin/logs",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		},
	}
	return append(systemVolMounts, volumeMounts...)
}

func (v *volumeFactory) NewVolumesForColumnar() []corev1.Volume {
	systemVols := v.NewSystemVolumes()
	volumes := []corev1.Volume{
		{
			Name:         "columnar-log",
			VolumeSource: v.newEmptyDirVolumeSource(),
		},
		{
			Name:         "columnar-config",
			VolumeSource: v.newEmptyDirVolumeSource(),
		},
	}

	nfsConfig := v.rc.Config().Nfs()
	if nfsConfig.Server() != "" {
		volumes = append(volumes, corev1.Volume{
			Name:         "polardbx-nfs",
			VolumeSource: v.newNFS(nfsConfig.Path(), nfsConfig.Server()),
		})
	}

	return append(systemVols, volumes...)
}

func (v *volumeFactory) NewVolumeMountsForColumnarEngine() []corev1.VolumeMount {
	systemVolMounts := v.NewSystemVolumeMounts()
	volumeMounts := []corev1.VolumeMount{
		{
			Name:             "columnar-log",
			MountPath:        "/home/admin/polardbx-columnar/logs",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		},
		//{
		//	Name:             "columnar-config",
		//	MountPath:        "/home/admin/polardbx-columnar/conf/config.properties",
		//	ReadOnly:         true,
		//	SubPath:          "config.properties",
		//	MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		//},
	}

	nfsConfig := v.rc.Config().Nfs()
	if nfsConfig.Server() != "" {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:             "polardbx-nfs",
			MountPath:        "/home/admin/polardbx-external-disk",
			MountPropagation: mountPropagationModePtr(corev1.MountPropagationHostToContainer),
		})
	}

	return append(systemVolMounts, volumeMounts...)
}

func NewVolumeFactory(rc *polardbxv1reconcile.Context, polardbx *polardbxv1.PolarDBXCluster) VolumeFactory {
	return &volumeFactory{rc: rc, polardbx: polardbx}
}
