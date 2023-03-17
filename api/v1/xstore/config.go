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

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/alibaba/polardbx-operator/api/v1/common"
)

type EngineConfig struct {
	Template *common.Value `json:"template,omitempty"`
	Override *common.Value `json:"override,omitempty"`
}

type ControllerConfig struct {
	LogPurgeInterval *metav1.Duration `json:"logPurgeInterval,omitempty"`

	// DiskQuota is the limit of data current node can use. This is a soft limit.
	// +optional
	DiskQuota         *resource.Quantity `json:"diskQuota,omitempty"`
	LogDataSeparation bool               `json:"logDataSeparation,omitempty"`

	// +kubebuilder:default=1
	// +kubebuilder:validation:Enum=1;2;"1";"2";""
	RpcProtocolVersion intstr.IntOrString `json:"RPCProtocolVersion,omitempty"`
}

type Config struct {
	Dynamic ControllerConfig              `json:"controller,omitempty"`
	Engine  EngineConfig                  `json:"engine,omitempty"`
	Envs    map[string]intstr.IntOrString `json:"envs,omitempty"`
}
