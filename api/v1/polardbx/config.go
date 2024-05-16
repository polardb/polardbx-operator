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

package polardbx

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type CNStaticConfig struct {
	AttendHtap bool `json:"AttendHtap,omitempty"`
	// +kubebuilder:default=true
	// +optional
	EnableCoroutine      bool                          `json:"EnableCoroutine"`
	EnableReplicaRead    bool                          `json:"EnableReplicaRead,omitempty"`
	EnableJvmRemoteDebug bool                          `json:"EnableJvmRemoteDebug,omitempty"`
	ServerProperties     map[string]intstr.IntOrString `json:"ServerProperties,omitempty"`

	// +kubebuilder:validation:Enum=1;2;"1";"2";""

	RPCProtocolVersion intstr.IntOrString `json:"RPCProtocolVersion,omitempty"`
}

type CNConfig struct {
	Dynamic map[string]intstr.IntOrString `json:"dynamic,omitempty"`

	// +kubebuilder:default={EnableCoroutine: true}

	Static         *CNStaticConfig `json:"static,omitempty"`
	EnableAuditLog bool            `json:"enableAuditLog,omitempty"`

	// ColdDataFileStorage defines the file storage used to store cold data
	ColdDataFileStorage []FileStorageInfo `json:"coldDataFileStorage,omitempty"`
}

type DNConfig struct {
	MycnfOverwrite    string                        `json:"mycnfOverwrite,omitempty"`
	LogPurgeInterval  metav1.Duration               `json:"logPurgeInterval,omitempty"`
	EnableAuditLog    bool                          `json:"enableAuditLog,omitempty"`
	LogDataSeparation bool                          `json:"logDataSeparation,omitempty"`
	Envs              map[string]intstr.IntOrString `json:"envs,omitempty"`
}

type CDCConfig struct {
	Envs map[string]intstr.IntOrString `json:"envs,omitempty"`
}

type ColumnarConfig struct {
	Envs map[string]intstr.IntOrString `json:"envs,omitempty"`

	// ColumnarDataFileStorage defines the file storage used to store cold data
	ColumnarDataFileStorage []FileStorageInfo `json:"columnarDataFileStorage,omitempty"`
}

type Config struct {
	// CN config.
	CN CNConfig `json:"cn,omitempty"`

	// DN config.
	DN DNConfig `json:"dn,omitempty"`

	// CDC config
	CDC CDCConfig `json:"cdc,omitempty"`

	// Columnar config
	Columnar ColumnarConfig `json:"columnar,omitempty"`
}
