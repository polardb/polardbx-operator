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
	"github.com/alibaba/polardbx-operator/pkg/util/network"
	corev1 "k8s.io/api/core/v1"

	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/factory"
)

type ExtraPodFactoryGalaxy struct {
	Delegate factory.ExtraPodFactory
}

func (f *ExtraPodFactoryGalaxy) NewPorts(ctx *factory.PodFactoryContext, allocated map[string]int) (map[string][]corev1.ContainerPort, error) {
	ports, err := f.Delegate.NewPorts(ctx, allocated)
	if err != nil {
		return nil, err
	}

	// Inject port "paxos" and "polarx"
	accessPort := k8shelper.GetPortFromPorts(
		ports[convention.ContainerEngine],
		convention.PortAccess,
	)
	ports[convention.ContainerEngine] = append(
		ports[convention.ContainerEngine],
		[]corev1.ContainerPort{
			{
				Name:          "paxos",
				ContainerPort: network.GetPaxosPort(accessPort.ContainerPort),
			},
			{
				Name:          "polarx",
				ContainerPort: network.GetXPort(accessPort.ContainerPort),
			},
		}...,
	)

	return ports, nil
}

func (f *ExtraPodFactoryGalaxy) NewVolumes(ctx *factory.PodFactoryContext, volumes []polardbxv1xstore.HostPathVolume) ([]corev1.Volume, error) {
	return f.Delegate.NewVolumes(ctx, volumes)
}

func (f *ExtraPodFactoryGalaxy) NewVolumeMounts(ctx *factory.PodFactoryContext) (map[string][]corev1.VolumeMount, error) {
	return f.Delegate.NewVolumeMounts(ctx)
}

func (f *ExtraPodFactoryGalaxy) NewEnvs(ctx *factory.PodFactoryContext) (map[string][]corev1.EnvVar, error) {
	envs, err := f.Delegate.NewEnvs(ctx)
	if err != nil {
		return nil, err
	}
	envs[convention.ContainerEngine] = k8shelper.PatchEnvs(envs[convention.ContainerEngine], []corev1.EnvVar{
		{Name: "ENGINE_HOME", Value: "/opt/galaxy_engine"},
	})
	return envs, nil
}

func (f *ExtraPodFactoryGalaxy) ExtraLabels(ctx *factory.PodFactoryContext) map[string]string {
	return f.Delegate.ExtraLabels(ctx)
}

func (f *ExtraPodFactoryGalaxy) ExtraAnnotations(ctx *factory.PodFactoryContext) map[string]string {
	return f.Delegate.ExtraAnnotations(ctx)
}

func (f *ExtraPodFactoryGalaxy) WorkDir(ctx *factory.PodFactoryContext, container string) string {
	return f.Delegate.WorkDir(ctx, container)
}

func (f *ExtraPodFactoryGalaxy) Command(ctx *factory.PodFactoryContext, container string) []string {
	// return []string{"bash", "-c", "while true; do sleep 3600; done"}
	return f.Delegate.Command(ctx, container)
}

func (f *ExtraPodFactoryGalaxy) NewProbes(ctx *factory.PodFactoryContext, container string) *factory.ProbeSpec {
	return f.Delegate.NewProbes(ctx, container)
}

func (f *ExtraPodFactoryGalaxy) NewResources(ctx *factory.PodFactoryContext, container string) corev1.ResourceRequirements {
	return f.Delegate.NewResources(ctx, container)
}

func (f *ExtraPodFactoryGalaxy) NewAffinity(ctx *factory.PodFactoryContext) *corev1.Affinity {
	return f.Delegate.NewAffinity(ctx)
}
