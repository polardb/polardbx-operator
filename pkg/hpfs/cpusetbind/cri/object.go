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

package cri

import runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

type ContainerLinux struct {
	Resources *runtimeapi.LinuxContainerResources `json:"resources,omitempty"`
}

type ContainerConfig struct {
	Linux *ContainerLinux `json:"linux,omitempty"`
}

type ContainerRuntimeSpecLinuxResourceCpu struct {
	Shares int64  `json:"shares,omitempty"`
	Quota  int64  `json:"quota,omitempty"`
	Period int64  `json:"period,omitempty"`
	Cpus   string `json:"cpus,omitempty"`
}

type ContainerRuntimeSpecLinuxResources struct {
	Cpu *ContainerRuntimeSpecLinuxResourceCpu `json:"cpu,omitempty"`
}

type ContainerRuntimeSpecLinux struct {
	Resources *ContainerRuntimeSpecLinuxResources `json:"resources,omitempty"`
}

type ContainerRuntimeSpec struct {
	OciVersion string                     `json:"ociVersion,omitempty"`
	Linux      *ContainerRuntimeSpecLinux `json:"linux,omitempty"`
}

type ContainerInfo struct {
	Config      *ContainerConfig      `json:"config,omitempty"`
	RuntimeSpec *ContainerRuntimeSpec `json:"runtimeSpec,omitempty"`
}
