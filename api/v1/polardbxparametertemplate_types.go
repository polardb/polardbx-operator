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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	CNReadOnly   = "CNReadOnly"
	CNReadWrite  = "CNReadWrite"
	CNRestart    = "CNRestart"
	DNReadOnly   = "DNReadOnly"
	DNReadWrite  = "DNReadWrite"
	DNRestart    = "DNRestart"
	GMSReadOnly  = "GMSReadOnly"
	GMSReadWrite = "GMSReadWrite"
	GMSRestart   = "GMSRestart"
)

const (
	UnitString     = "STRING"
	UnitInt        = "INT"
	UnitDouble     = "DOUBLE"
	UnitTZ         = "TZ"
	UnitHOUR_RANGE = "HOUR_RANGE"
)

// Mode
const (
	ReadOnly  = "readonly"
	ReadWrite = "readwrite"
)

type TemplateParams struct {
	Name               string `json:"name,omitempty"`
	DefaultValue       string `json:"defaultValue,omitempty"`
	Mode               string `json:"mode,omitempty"`
	Restart            bool   `json:"restart,omitempty"`
	Unit               string `json:"unit,omitempty"`
	DivisibilityFactor int64  `json:"divisibilityFactor,omitempty"`
	Optional           string `json:"optional,omitempty"`
}

type TemplateNode struct {
	// +kubebuilder:validation:Required
	Name string `json:"name,omitempty"`
	// +kubebuilder:validation:Required
	ParamList []TemplateParams `json:"paramList,omitempty"`
}

type TemplateNodeType struct {
	// +optional
	CN TemplateNode `json:"cn,omitempty"`
	// +optional
	DN TemplateNode `json:"dn,omitempty"`
	// +optional
	GMS *TemplateNode `json:"gms,omitempty"`
}

type PolarDBXParameterTemplateSpec struct {
	// TemplateName represents the service name of the parameter template. Default is the same as the name.
	// +optional
	Name string `json:"name,omitempty"`

	// NodeType represents the type of the node parameters runs on
	NodeType TemplateNodeType `json:"nodeType"`
}

type PolarDBXParameterTemplateStatus struct {
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=pxpt;polardbxpt;polardbxparametertemplate
// +kubebuilder:subresource:status

type PolarDBXParameterTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PolarDBXParameterTemplateSpec   `json:"spec,omitempty"`
	Status PolarDBXParameterTemplateStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PolarDBXParameterTemplateList contains a list of PolarDBXParameterTemplate.
type PolarDBXParameterTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PolarDBXParameterTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PolarDBXParameterTemplate{}, &PolarDBXParameterTemplateList{})
}
