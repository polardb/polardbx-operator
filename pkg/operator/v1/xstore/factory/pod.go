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
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	boolutil "github.com/alibaba/polardbx-operator/pkg/util/bool"
	"github.com/alibaba/polardbx-operator/pkg/util/defaults"
)

type PodFactoryContext struct {
	rc       *reconcile.Context
	xstore   *polardbxv1.XStore
	engine   string
	nodeSet  *polardbxv1xstore.NodeSet
	index    int
	podName  string
	template *polardbxv1xstore.NodeTemplate
	ports    map[string][]corev1.ContainerPort
	portMap  map[string]int
	volumes  []polardbxv1xstore.HostPathVolume
	envs     map[string][]corev1.EnvVar
}

type ExtraPodFactory interface {
	NewPorts(ctx *PodFactoryContext, allocated map[string]int) (map[string][]corev1.ContainerPort, error)
	NewVolumes(ctx *PodFactoryContext, volumes []polardbxv1xstore.HostPathVolume) ([]corev1.Volume, error)
	NewVolumeMounts(ctx *PodFactoryContext) (map[string][]corev1.VolumeMount, error)
	NewEnvs(ctx *PodFactoryContext) (map[string][]corev1.EnvVar, error)
	ExtraLabels(ctx *PodFactoryContext) map[string]string
	ExtraAnnotations(ctx *PodFactoryContext) map[string]string
	WorkDir(ctx *PodFactoryContext, container string) string
	Command(ctx *PodFactoryContext, container string) []string
	NewProbes(ctx *PodFactoryContext, container string) *ProbeSpec
	NewResources(ctx *PodFactoryContext, container string) corev1.ResourceRequirements
	NewAffinity(ctx *PodFactoryContext) *corev1.Affinity
}

type ProbeSpec struct {
	StartupProbe   *corev1.Probe
	LivenessProbe  *corev1.Probe
	ReadinessProbe *corev1.Probe
}

func (p *ProbeSpec) Setup(c *corev1.Container) {
	if p == nil {
		return
	}
	c.StartupProbe = p.StartupProbe.DeepCopy()
	c.LivenessProbe = p.LivenessProbe.DeepCopy()
	c.ReadinessProbe = p.ReadinessProbe.DeepCopy()
}

type TemplateMergePolicy int

const (
	TemplateMergePolicyOverwrite TemplateMergePolicy = iota
	TemplateMergePolicyPatch
)

type PodFactoryOptions struct {
	ExtraPodFactory
	TemplateMergePolicy
}

func PatchNodeTemplate(origin *polardbxv1xstore.NodeTemplate, overlay *polardbxv1xstore.NodeTemplate, overwrite bool) *polardbxv1xstore.NodeTemplate {
	if overlay == nil {
		return origin
	}
	if origin == nil {
		return overlay
	}

	// Overwrite or merge
	if overwrite {
		overlay.ObjectMeta.DeepCopyInto(&origin.ObjectMeta)
		origin.Spec = overlay.Spec
	} else {
		k8shelper.PatchLabels(origin.ObjectMeta.Labels, overlay.ObjectMeta.Labels)
		k8shelper.PatchAnnotations(origin.ObjectMeta.Annotations, overlay.ObjectMeta.Annotations)
		// Only affinity is merged.
		affinity := origin.Spec.Affinity
		origin.Spec = overlay.Spec
		origin.Spec.Affinity = k8shelper.PatchAffinity(affinity, overlay.Spec.Affinity)
	}
	return origin
}

func NewPod(rc *reconcile.Context, xstore *polardbxv1.XStore, nodeSet *polardbxv1xstore.NodeSet,
	index int, opts PodFactoryOptions) (*corev1.Pod, error) {
	engine := xstore.Spec.Engine

	// We use the observed topology instead of topology in spec
	// to avoid unstable changes to spec. It shall be disabled by webhook,
	// but now it's left unimplemented.
	topology := xstore.Status.ObservedTopology
	generation := xstore.Status.ObservedGeneration

	// Stable pod name by convention.
	podName := convention.NewPodName(xstore, nodeSet, index)

	// Get current template by applying the merge policy
	template := PatchNodeTemplate(topology.Template.DeepCopy(), nodeSet.Template,
		opts.TemplateMergePolicy == TemplateMergePolicyOverwrite)

	// Setup context
	factoryCtx := &PodFactoryContext{
		rc:       rc,
		xstore:   xstore,
		engine:   engine,
		nodeSet:  nodeSet,
		index:    index,
		podName:  podName,
		template: template,
	}

	// Volumes must not be nil.
	hostPathVolume := xstore.Status.BoundVolumes[podName]
	if hostPathVolume == nil {
		panic("volume must be pre-allocated")
	}

	// Generate container ports with either allocated ports or new.
	allocatedPorts := xstore.Status.PodPorts[podName].ToMap()
	containerPorts, err := opts.NewPorts(factoryCtx, allocatedPorts)
	if err != nil {
		return nil, err
	}
	factoryCtx.ports = containerPorts

	allocatedPorts = make(map[string]int)
	for _, ports := range containerPorts {
		for _, port := range ports {
			allocatedPorts[port.Name] = int(port.ContainerPort)
		}
	}
	factoryCtx.portMap = allocatedPorts

	// Generate volumes used by pod.
	hostPathVolumes := []polardbxv1xstore.HostPathVolume{*hostPathVolume}
	volumes, err := opts.NewVolumes(factoryCtx, hostPathVolumes)
	if err != nil {
		return nil, err
	}
	factoryCtx.volumes = hostPathVolumes

	// Generate volume mounts used by different containers.
	volumeMounts, err := opts.NewVolumeMounts(factoryCtx)
	if err != nil {
		return nil, err
	}

	// Generate envs
	envs, err := opts.NewEnvs(factoryCtx)
	if err != nil {
		return nil, err
	}
	factoryCtx.envs = envs

	// Construct the pod.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: xstore.Namespace,
			Labels: k8shelper.PatchLabels(
				template.ObjectMeta.Labels,
				opts.ExtraLabels(factoryCtx),
				convention.ConstPodLabels(xstore, nodeSet),
				map[string]string{
					xstoremeta.LabelPod:        podName,
					xstoremeta.LabelRole:       convention.DefaultRoleOf(nodeSet.Role),
					xstoremeta.LabelGeneration: strconv.FormatInt(generation, 10),
				},
			),
			Annotations: k8shelper.PatchAnnotations(
				template.ObjectMeta.Annotations,
				opts.ExtraAnnotations(factoryCtx),
			),
		},
		Spec: corev1.PodSpec{
			Volumes: k8shelper.PatchVolumes(
				SystemVolumes(),
				ConfigMapVolumes(xstore),
				volumes,
			),
			EnableServiceLinks:            pointer.BoolPtr(false),
			ImagePullSecrets:              template.Spec.ImagePullSecrets,
			DNSPolicy:                     corev1.DNSClusterFirstWithHostNet,
			RestartPolicy:                 corev1.RestartPolicyAlways,
			TerminationGracePeriodSeconds: pointer.Int64Ptr(300),
			HostNetwork:                   boolutil.IsTrue(template.Spec.HostNetwork),
			ShareProcessNamespace:         pointer.BoolPtr(true),
			Affinity:                      opts.NewAffinity(factoryCtx),
			Tolerations:                   xstore.Spec.Tolerations,
			NodeName:                      hostPathVolume.Host, // If already bound, then assign to the same host.
			Containers: []corev1.Container{
				{
					Name: convention.ContainerEngine,
					Image: defaults.NonEmptyStrOrDefault(
						template.Spec.Image,
						rc.Config().Images().DefaultImageForStore(engine, convention.ContainerEngine, ""),
					),
					ImagePullPolicy: template.Spec.ImagePullPolicy,
					Ports:           containerPorts[convention.ContainerEngine],
					WorkingDir:      opts.WorkDir(factoryCtx, convention.ContainerEngine),
					Command:         opts.Command(factoryCtx, convention.ContainerEngine),
					Resources:       opts.NewResources(factoryCtx, convention.ContainerEngine),
					VolumeMounts: k8shelper.PatchVolumeMounts(
						SystemVolumeMounts(),
						ConfigMapVolumeMounts(xstore),
						volumeMounts[convention.ContainerEngine],
					),
					Env:             k8shelper.PatchEnvs(SystemEnvs(), envs[convention.ContainerEngine]),
					SecurityContext: k8shelper.NewSecurityContext(rc.Config().Store().ContainerPrivileged()),
					Lifecycle: &corev1.Lifecycle{
						PreStop: &corev1.LifecycleHandler{
							Exec: &corev1.ExecAction{
								Command: []string{
									"/tools/xstore/current/venv/bin/python3", "/tools/xstore/current/cli.py", "engine", "shutdown",
								},
							},
						},
					},
				},
				{
					Name:         convention.ContainerExporter,
					Image:        rc.Config().Images().DefaultImageForStore(engine, convention.ContainerExporter, ""),
					Ports:        containerPorts[convention.ContainerExporter],
					VolumeMounts: SystemVolumeMounts(),
					Args: []string{
						"--web.listen-address", fmt.Sprintf(":%d", allocatedPorts[convention.PortMetrics]),
						"--web.telemetry-path", "/metrics",
						"--collect.engine_innodb_status",
						"--collect.info_schema.innodb_metrics",
						"--collect.info_schema.innodb_tablespaces",
						"--collect.info_schema.processlist",
						"--collect.info_schema.query_response_time",
					},
					Env: k8shelper.PatchEnvs(SystemEnvs(),
						envs[convention.ContainerExporter],
						[]corev1.EnvVar{
							{Name: "DATA_SOURCE_NAME", Value: fmt.Sprintf("root:@(127.0.0.1:%d)/", allocatedPorts[convention.PortAccess])},
						},
					),
					Resources: opts.NewResources(factoryCtx, convention.ContainerExporter),
				},
				{
					Name:         convention.ContainerProber,
					Image:        rc.Config().Images().DefaultImageForStore(engine, convention.ContainerProber, ""),
					Ports:        containerPorts[convention.ContainerProber],
					VolumeMounts: append(SystemVolumeMounts(), volumeMounts[convention.ContainerProber]...),
					Args: []string{
						"--listen-port", fmt.Sprintf("%d", allocatedPorts[convention.PortProbe]),
						"--feature-gates", featuregate.ExtraFeatureGateArg(),
					},
					Env:       k8shelper.PatchEnvs(SystemEnvs(), envs[convention.ContainerProber]),
					Resources: opts.NewResources(factoryCtx, convention.ContainerExporter),
				},
			},
		},
	}

	// Setup probes.
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		p := opts.NewProbes(factoryCtx, c.Name)
		p.Setup(c)
	}

	return pod, nil
}
