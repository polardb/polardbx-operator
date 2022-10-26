/*
Copyright 2022 Alibaba Group Holding Limited.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type LogCollectorSpec struct {
	FileBeatName string `json:"fileBeatName,omitempty"`
	LogStashName string `json:"logStashName,omitempty"`
}

type LogCollectorConfigStatus struct {
	FileBeatReadyCount int32  `json:"fileBeatReadyCount,omitempty"`
	FileBeatCount      int32  `json:"fileBeatCount,omitempty"`
	FileBeatConfigId   string `json:"fileBeatConfigId,omitempty"`
	LogStashReadyCount int32  `json:"logStashReadyCount,omitempty"`
	LogStashCount      int32  `json:"logStashCount,omitempty"`
	LogStashConfigId   string `json:"logStashConfigId,omitempty"`
}

type LogCollectorStatus struct {
	ConfigStatus *LogCollectorConfigStatus `json:"configStatus,omitempty"`
	SpecSnapshot *LogCollectorSpec         `json:"specSnapshot,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=pxlc;polardbxlogcollector
// +kubebuilder:printcolumn:name="NS",type=string,JSONPath=`.metadata.namespace`
// +kubebuilder:printcolumn:name="NAME",type=string,JSONPath=`.metadata.name`
// +kubebuilder:printcolumn:name="FB_NAME",type=string,JSONPath=`.spec.fileBeatName`
// +kubebuilder:printcolumn:name="FB_CNT",type=integer,JSONPath=`.status.configStatus.fileBeatReadyCount`
// +kubebuilder:printcolumn:name="LS_NAME",type=string,JSONPath=`.spec.logStashName`
// +kubebuilder:printcolumn:name="LS_CNT",type=string,JSONPath=`.status.configStatus.logStashReadyCount`
type PolarDBXLogCollector struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              LogCollectorSpec   `json:"spec,omitempty"`
	Status            LogCollectorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type PolarDBXLogCollectorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PolarDBXLogCollector `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PolarDBXLogCollector{}, &PolarDBXLogCollectorList{})
}
