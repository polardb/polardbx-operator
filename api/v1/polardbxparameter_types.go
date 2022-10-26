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

package v1

import (
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Params struct {
	Name  string `json:"name,omitempty"`
	Value string `json:"value,omitempty"`
}

type ParamNode struct {
	// +kubebuilder:validation:Required
	Name string `json:"name,omitempty"`
	// +optional
	RestartType string `json:"restartType,omitempty"`
	// +kubebuilder:validation:Required
	ParamList []Params `json:"paramList,omitempty"`
}

type ParamNodeType struct {
	// +optional
	CN ParamNode `json:"cn,omitempty"`
	// +optional
	DN ParamNode `json:"dn,omitempty"`
	// +optional
	//If not provided, the operator will use the paramNode for DN as template for GMS.
	GMS *ParamNode `json:"gms,omitempty"`
}

type PolarDBXParameterSpec struct {
	ClusterName string `json:"clusterName"`
	// TemplateName represents the service name of the template name. Default is the same as the name.
	// +optional
	TemplateName string `json:"templateName,omitempty"`

	// NodeType represents the type of the node parameters runs on
	NodeType ParamNodeType `json:"nodeType"`
}

type PolarDBXParameterStatus struct {
	// Phase is the current phase of the cluster.
	Phase polardbxv1polardbx.ParameterPhase `json:"phase,omitempty"`

	// PrevParameterSpecSnapshot represents the previous version snapshot of the parameter.
	PrevParameterSpecSnapshot *PolarDBXParameterSpec `json:"prevParameterSpecSnapshot,omitempty"`

	// ParameterSpecSnapshot represents the snapshot of the parameter.
	ParameterSpecSnapshot *PolarDBXParameterSpec `json:"parameterSpecSnapshot,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=pxp;polardbxp;polardbxparameter
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="PHASE",type=string,JSONPath=`.status.phase`

type PolarDBXParameter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PolarDBXParameterSpec   `json:"spec,omitempty"`
	Status PolarDBXParameterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PolarDBXParameterList contains a list of PolarDBXParameter.
type PolarDBXParameterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PolarDBXParameter `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PolarDBXParameter{}, &PolarDBXParameterList{})
}
