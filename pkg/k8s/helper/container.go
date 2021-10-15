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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"
)

func GetPortFromPorts(ports []corev1.ContainerPort, name string) *corev1.ContainerPort {
	if ports == nil {
		return nil
	}
	for i := range ports {
		port := &ports[i]
		if name == port.Name {
			return port
		}
	}
	return nil
}

func GetPortFromContainer(container *corev1.Container, name string) *corev1.ContainerPort {
	if container == nil {
		return nil
	}
	return GetPortFromPorts(container.Ports, name)
}

func MustGetPortFromContainer(container *corev1.Container, name string) *corev1.ContainerPort {
	port := GetPortFromContainer(container, name)
	if port == nil {
		panic("port not found: " + name)
	}
	return port
}

func NewSecurityContext(privileged bool) *corev1.SecurityContext {
	if privileged {
		return &corev1.SecurityContext{
			Privileged: pointer.Bool(true),
			Capabilities: &corev1.Capabilities{
				Add: []corev1.Capability{
					"SYS_PTRACE",
					"SYS_ADMIN",
				},
			},
		}
	} else {
		return nil
	}
}
