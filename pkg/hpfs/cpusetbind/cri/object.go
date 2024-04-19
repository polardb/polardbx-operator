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
