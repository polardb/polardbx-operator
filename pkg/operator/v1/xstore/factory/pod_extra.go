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
	"path"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/rand"

	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/probe"
	boolutil "github.com/alibaba/polardbx-operator/pkg/util/bool"
	"github.com/alibaba/polardbx-operator/pkg/util/defaults"
)

type DefaultExtraPodFactory struct {
}

func (f *DefaultExtraPodFactory) NewAffinity(ctx *PodFactoryContext) *corev1.Affinity {
	xstore := ctx.xstore
	template := ctx.template

	// Always try scatter between hosts.
	affinity := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{Weight: 100, PodAffinityTerm: corev1.PodAffinityTerm{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: convention.ConstLabels(xstore),
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      xstoremeta.LabelNodeRole,
								Operator: metav1.LabelSelectorOpIn,
								Values: []string{
									strings.ToLower(string(polardbxv1xstore.RoleCandidate)),
									strings.ToLower(string(polardbxv1xstore.RoleVoter)),
								},
							},
						},
					},
					Namespaces:  []string{xstore.Namespace},
					TopologyKey: corev1.LabelHostname,
				}},
			},
		},
	}

	// If host network, set anti affinity on lock (access) port.
	if boolutil.IsTrue(template.Spec.HostNetwork) {
		affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{
			{
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					xstoremeta.LabelPortLock: strconv.Itoa(ctx.portMap[convention.PortAccess]),
				}},
				Namespaces:  []string{xstore.Namespace},
				TopologyKey: corev1.LabelHostname,
			},
		}
	}

	// Patch with template's affinity.
	affinity = k8shelper.PatchAffinity(affinity, template.Spec.Affinity)

	// Set the master restriction, disallow scheduling to master.
	if !ctx.rc.Config().Scheduler().AllowScheduleToMasterNode() {
		nodeSelectorTermNotOnMaster := corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{
				{
					Key:      k8shelper.LabelNodeRole,
					Operator: corev1.NodeSelectorOpNotIn,
					Values:   []string{k8shelper.NodeRoleMaster},
				},
			},
		}

		if affinity.NodeAffinity != nil {
			if affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
				if affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms == nil {
					affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = []corev1.NodeSelectorTerm{
						nodeSelectorTermNotOnMaster,
					}
				} else {
					for i := range affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
						term := &affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i]
						// Nil slice can be appended
						term.MatchExpressions = append(term.MatchExpressions, nodeSelectorTermNotOnMaster.MatchExpressions...)
					}
				}
			} else {
				affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						nodeSelectorTermNotOnMaster,
					},
				}
			}
		} else {
			affinity.NodeAffinity = &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						nodeSelectorTermNotOnMaster,
					},
				},
			}
		}
	}

	return affinity
}

func (f *DefaultExtraPodFactory) NewPorts(ctx *PodFactoryContext, allocated map[string]int) (map[string][]corev1.ContainerPort, error) {
	if boolutil.IsTrue(ctx.template.Spec.HostNetwork) {
		// Default service node port range is 30000-32767, avoid if host network.
		return map[string][]corev1.ContainerPort{
			convention.ContainerEngine: {
				{
					Name: convention.PortAccess,
					ContainerPort: int32(defaults.GetIntOrDefault(
						allocated,
						convention.PortAccess,
						rand.IntnRange(14000, 18000),
					)),
				},
			},
			convention.ContainerExporter: {
				{
					Name: convention.PortMetrics,
					ContainerPort: int32(defaults.GetIntOrDefault(
						allocated,
						convention.PortMetrics,
						rand.IntnRange(20000, 22000),
					)),
				},
			},
			convention.ContainerProber: {
				{
					Name: convention.PortProbe,
					ContainerPort: int32(defaults.GetIntOrDefault(
						allocated,
						convention.PortProbe,
						rand.IntnRange(22000, 24000),
					)),
				},
			},
		}, nil
	} else {
		return map[string][]corev1.ContainerPort{
			convention.ContainerEngine: {
				{
					Name: convention.PortAccess,
					ContainerPort: int32(defaults.GetIntOrDefault(
						allocated, convention.PortAccess, 3306),
					),
				},
			},
			convention.ContainerExporter: {
				{
					Name: convention.PortMetrics,
					ContainerPort: int32(defaults.GetIntOrDefault(
						allocated, convention.PortMetrics, 8000),
					),
				},
			},
			convention.ContainerProber: {
				{
					Name: convention.PortProbe,
					ContainerPort: int32(defaults.GetIntOrDefault(
						allocated, convention.PortProbe, 8081),
					),
				},
			},
		}, nil
	}
}

func (f *DefaultExtraPodFactory) newDataVolumeName(i int, skipSequence bool) string {
	if skipSequence {
		return "data"
	}
	return "data-" + strconv.Itoa(i)
}

func (f *DefaultExtraPodFactory) newLogVolumeName(i int, skipSequence bool) string {
	if skipSequence {
		return "data-log"
	}
	return "data-log" + strconv.Itoa(i)
}

func (f *DefaultExtraPodFactory) newDataVolumeMountPath(i int, skipSequence bool) string {
	if skipSequence {
		return "/data/mysql"
	}
	return path.Join("/data/mysql", strconv.Itoa(i))
}

func (f *DefaultExtraPodFactory) newLogVolumeMountPath(i int, skipSequence bool) string {
	if skipSequence {
		return "/data-log/mysql"
	}
	return path.Join("/data-log/mysql", strconv.Itoa(i))
}

func newHostPathTypeForHostPathVolume(vol *polardbxv1xstore.HostPathVolume) *corev1.HostPathType {
	switch vol.Type {
	case corev1.HostPathDirectory:
		return k8shelper.HostPathTypePtr(corev1.HostPathDirectoryOrCreate)
	case corev1.HostPathFile:
		return k8shelper.HostPathTypePtr(corev1.HostPathFileOrCreate)
	default:
		panic("unrecognized host path type.")
	}
}

func (f *DefaultExtraPodFactory) NewVolumes(ctx *PodFactoryContext, volumes []polardbxv1xstore.HostPathVolume) ([]corev1.Volume, error) {
	skipSequence := true
	if len(volumes) > 1 {
		skipSequence = false
	}

	res := make([]corev1.Volume, 0, len(volumes)+1)
	res = append(res, corev1.Volume{
		Name: "xstore-tools",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: ctx.rc.Config().Store().HostPathTools(),
				Type: k8shelper.HostPathTypePtr(corev1.HostPathDirectory),
			},
		},
	})

	for i, vol := range volumes {
		res = append(res, corev1.Volume{
			Name: f.newDataVolumeName(i, skipSequence),
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: vol.HostPath,
					Type: newHostPathTypeForHostPathVolume(&vol),
				},
			},
		})
		res = append(res, corev1.Volume{
			Name: f.newLogVolumeName(i, skipSequence),
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: vol.LogHostPath,
					Type: newHostPathTypeForHostPathVolume(&vol),
				},
			},
		})
	}

	return res, nil
}

func (f *DefaultExtraPodFactory) NewVolumeMounts(ctx *PodFactoryContext) (map[string][]corev1.VolumeMount, error) {
	skipSequence := true
	if len(ctx.volumes) > 1 {
		skipSequence = false
	}

	mounts := make([]corev1.VolumeMount, 0, len(ctx.volumes)+1)
	mounts = append(mounts, corev1.VolumeMount{
		Name:      "xstore-tools",
		ReadOnly:  true,
		MountPath: "/tools/xstore",
	})

	for i := range ctx.volumes {
		mounts = append(mounts, corev1.VolumeMount{
			Name:             f.newDataVolumeName(i, skipSequence),
			MountPath:        f.newDataVolumeMountPath(i, skipSequence),
			MountPropagation: k8shelper.MountPropagationModePtr(corev1.MountPropagationHostToContainer),
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name:             f.newLogVolumeName(i, skipSequence),
			MountPath:        f.newLogVolumeMountPath(i, skipSequence),
			MountPropagation: k8shelper.MountPropagationModePtr(corev1.MountPropagationHostToContainer),
		})
	}

	return map[string][]corev1.VolumeMount{
		convention.ContainerEngine: mounts,
	}, nil
}

func (f *DefaultExtraPodFactory) newEnvsForEnginePorts(ctx *PodFactoryContext) []corev1.EnvVar {
	portEnvs := make([]corev1.EnvVar, 0)
	for _, port := range ctx.ports[convention.ContainerEngine] {
		portEnvs = append(portEnvs, corev1.EnvVar{
			Name:  "PORT_" + strings.ToUpper(port.Name),
			Value: strconv.FormatInt(int64(port.ContainerPort), 10),
		})
	}
	return portEnvs
}

func (f *DefaultExtraPodFactory) NewEnvs(ctx *PodFactoryContext) (map[string][]corev1.EnvVar, error) {
	nodeSet := ctx.nodeSet
	template := ctx.template
	resources := template.Spec.Resources

	return map[string][]corev1.EnvVar{
		convention.ContainerEngine: k8shelper.PatchEnvs(
			[]corev1.EnvVar{
				{Name: "NODE_ROLE", Value: strings.ToLower(string(nodeSet.Role))},
				{Name: "ENGINE", Value: ctx.engine},
				{Name: "LIMITS_CPU", Value: strconv.FormatInt(resources.Limits.Cpu().MilliValue(), 10)},
				{Name: "LIMITS_MEM", Value: strconv.FormatInt(resources.Limits.Memory().Value(), 10)},
				{Name: "LOG_DATA_SEPARATION", Value: strconv.FormatBool(ctx.xstore.Spec.Config.Dynamic.LogDataSeparation)},
			},
			f.newEnvsForEnginePorts(ctx),
		),
	}, nil
}

func (f *DefaultExtraPodFactory) ExtraLabels(ctx *PodFactoryContext) map[string]string {
	extraLabels := make(map[string]string, 0)
	if boolutil.IsTrue(ctx.template.Spec.HostNetwork) {
		// Lock on access port to avoid port conflict when host network is true.
		extraLabels[xstoremeta.LabelPortLock] = strconv.Itoa(ctx.portMap[convention.PortAccess])
	}
	config := xstoremeta.RebuildConfig{
		LogSeparation: strconv.FormatBool(ctx.xstore.Spec.Config.Dynamic.LogDataSeparation),
	}
	extraLabels[xstoremeta.LabelConfigHash] = config.ComputeHash()
	return extraLabels
}

func (f *DefaultExtraPodFactory) ExtraAnnotations(ctx *PodFactoryContext) map[string]string {
	return nil
}

func (f *DefaultExtraPodFactory) WorkDir(ctx *PodFactoryContext, container string) string {
	switch container {
	case convention.ContainerEngine:
		return "/"
	case convention.ContainerExporter:
		return ""
	case convention.ContainerProber:
		return ""
	default:
		panic("invalid container: " + container)
	}
}

func (f *DefaultExtraPodFactory) Command(ctx *PodFactoryContext, container string) []string {
	switch container {
	case convention.ContainerEngine:
		return command.NewCanonicalCommandBuilder().Entrypoint().Start().Build()
	case convention.ContainerExporter:
		return nil
	case convention.ContainerProber:
		return nil
	default:
		panic("invalid container: " + container)
	}
}

func (f *DefaultExtraPodFactory) newProbeHandlerViaProber(endpoint, target string, serverPort, mysqlPort int, extra string) corev1.Handler {
	return corev1.Handler{
		HTTPGet: &corev1.HTTPGetAction{
			Path: endpoint,
			Port: intstr.FromInt(serverPort),
			HTTPHeaders: []corev1.HTTPHeader{
				{
					Name:  "Probe-Target",
					Value: target,
				},
				{
					Name:  "Probe-Port",
					Value: strconv.Itoa(mysqlPort),
				},
				{
					Name:  "Probe-Extra",
					Value: extra,
				},
				{
					Name:  "Probe-Timeout",
					Value: "10s",
				},
			},
		},
	}
}

func (f *DefaultExtraPodFactory) NewProbes(ctx *PodFactoryContext, container string) *ProbeSpec {
	switch container {
	case convention.ContainerEngine:
		return &ProbeSpec{
			StartupProbe: &corev1.Probe{
				Handler: f.newProbeHandlerViaProber("/liveness", probe.TypeXStore,
					ctx.portMap[convention.PortProbe], ctx.portMap[convention.PortAccess], ctx.engine),
				InitialDelaySeconds: 5,
				TimeoutSeconds:      10,
				PeriodSeconds:       10,
				FailureThreshold:    360,
			},
			LivenessProbe: &corev1.Probe{
				Handler: f.newProbeHandlerViaProber("/liveness", probe.TypeXStore,
					ctx.portMap[convention.PortProbe], ctx.portMap[convention.PortAccess], ctx.engine),
				TimeoutSeconds: 10,
				PeriodSeconds:  10,
			},
			ReadinessProbe: &corev1.Probe{
				Handler: f.newProbeHandlerViaProber("/readiness", probe.TypeXStore,
					ctx.portMap[convention.PortProbe], ctx.portMap[convention.PortAccess], ctx.engine),
				TimeoutSeconds: 10,
				PeriodSeconds:  10,
			},
		}
	case convention.ContainerExporter:
		return &ProbeSpec{
			StartupProbe: &corev1.Probe{
				Handler: corev1.Handler{
					TCPSocket: &corev1.TCPSocketAction{
						Port: intstr.FromString(convention.PortMetrics),
					},
				},
				InitialDelaySeconds: 10,
				TimeoutSeconds:      10,
				PeriodSeconds:       30,
				FailureThreshold:    360,
			},
			LivenessProbe: &corev1.Probe{
				Handler: corev1.Handler{
					TCPSocket: &corev1.TCPSocketAction{
						Port: intstr.FromString(convention.PortMetrics),
					},
				},
				TimeoutSeconds: 10,
				PeriodSeconds:  30,
			},
			ReadinessProbe: &corev1.Probe{
				Handler: corev1.Handler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/metrics",
						Port: intstr.FromString(convention.PortMetrics),
					},
				},
				TimeoutSeconds: 10,
				PeriodSeconds:  30,
			},
		}
	case convention.ContainerProber:
		return &ProbeSpec{
			LivenessProbe: &corev1.Probe{
				Handler: f.newProbeHandlerViaProber("/liveness", probe.TypeSelf,
					ctx.portMap[convention.PortProbe], 0, ctx.engine),
				TimeoutSeconds: 5,
				PeriodSeconds:  30,
			},
		}
	default:
		panic("invalid container: " + container)
	}
}

func (f *DefaultExtraPodFactory) NewResources(ctx *PodFactoryContext, container string) corev1.ResourceRequirements {
	resources := ctx.template.Spec.Resources

	switch container {
	case convention.ContainerEngine:
		return *resources.ResourceRequirements.DeepCopy()
	case convention.ContainerExporter:
		if k8shelper.IsResourceQoSGuaranteed(resources.ResourceRequirements) {
			if featuregate.EnforceQoSGuaranteed.Enabled() {
				return corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("200m"),
						corev1.ResourceMemory: resource.MustParse("200Mi"),
					},
				}
			} else {
				return corev1.ResourceRequirements{}
			}
		} else {
			return corev1.ResourceRequirements{}
		}
	case convention.ContainerProber:
		if k8shelper.IsResourceQoSGuaranteed(resources.ResourceRequirements) {
			if featuregate.EnforceQoSGuaranteed.Enabled() {
				return corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("200m"),
						corev1.ResourceMemory: resource.MustParse("100Mi"),
					},
				}
			} else {
				return corev1.ResourceRequirements{}
			}
		} else {
			return corev1.ResourceRequirements{}
		}
	default:
		panic("invalid container: " + container)
	}
}
