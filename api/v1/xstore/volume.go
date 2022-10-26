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

package xstore

import corev1 "k8s.io/api/core/v1"

type HostPathVolume struct {
	// Host or node name if the volume is bound to some node.
	Host string `json:"host,omitempty"`

	// Pod if the volume is bound to some pod.
	Pod string `json:"pod,omitempty"`

	//Data  HostPath of the file/dir.
	HostPath string `json:"hostPath,omitempty"`

	//Log HostPath of the file/dir
	LogHostPath string `json:"logHostPath,omitempty"`

	// Type of the host path.
	Type corev1.HostPathType `json:"type,omitempty"`

	// Size of the volume.
	Size int64 `json:"size,omitempty"`

	// Size of the log volume.
	LogSize int64 `json:"logSize,omitempty"`

	// Size of the data volume.
	DataSize int64 `json:"dataSize,omitempty"`
}
