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
	"k8s.io/apimachinery/pkg/util/intstr"
)

type PolarDBXClusterKnobsSpec struct {
	ClusterName string                        `json:"clusterName,omitempty"`
	Knobs       map[string]intstr.IntOrString `json:"knobs,omitempty"`
}

type PolarDBXClusterKnobsStatus struct {
	Size        int32       `json:"size,omitempty"`
	LastUpdated metav1.Time `json:"lastUpdated,omitempty"`
	Version     int64       `json:"version,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=pxcknobs;polardbxknobs
// +kubebuilder:printcolumn:name="CLUSTER",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="VER",type=integer,JSONPath=`.status.version`
// +kubebuilder:printcolumn:name="SIZE",type=integer,JSONPath=`.status.size`
// +kubebuilder:printcolumn:name="SYNC",type=date,JSONPath=`.status.lastUpdated`
// +kubebuilder:printcolumn:name="AGE",type=date,JSONPath=`.metadata.creationTimestamp`

type PolarDBXClusterKnobs struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PolarDBXClusterKnobsSpec   `json:"spec,omitempty"`
	Status PolarDBXClusterKnobsStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type PolarDBXClusterKnobsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PolarDBXClusterKnobs `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PolarDBXClusterKnobs{}, &PolarDBXClusterKnobsList{})
}
