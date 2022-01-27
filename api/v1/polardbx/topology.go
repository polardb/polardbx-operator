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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/alibaba/polardbx-operator/api/v1/common"
	"github.com/alibaba/polardbx-operator/api/v1/xstore"
)

type NodeSelectorItem struct {
	Name         string              `json:"name,omitempty"`
	NodeSelector corev1.NodeSelector `json:"nodeSelector,omitempty"`
}

type NodeSelectorReference struct {
	Reference    string               `json:"reference,omitempty"`
	NodeSelector *corev1.NodeSelector `json:"selector,omitempty"`
}

type StatelessTopologyRuleItem struct {
	Name         string                 `json:"name,omitempty"`
	Replicas     *intstr.IntOrString    `json:"replicas,omitempty"`
	NodeSelector *NodeSelectorReference `json:"selector,omitempty"`
}

type XStoreTopologyRuleRolling struct {
	Replicas     int32                  `json:"replicas,omitempty"`
	NodeSelector *NodeSelectorReference `json:"selector,omitempty"`
}

type XStoreTopologyRuleNodeSetItem struct {
	Name string `json:"name,omitempty"`

	// +kubebuilder:validation:Enum=Candidate;Voter;Learner
	Role xstore.NodeRole `json:"role,omitempty"`

	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	Replicas     int32                  `json:"replicas,omitempty"`
	NodeSelector *NodeSelectorReference `json:"selector,omitempty"`
}

type XStoreTopologyRule struct {
	Rolling  *XStoreTopologyRuleRolling      `json:"rolling,omitempty"`
	NodeSets []XStoreTopologyRuleNodeSetItem `json:"nodeSets,omitempty"`
}

type TopologyRuleComponents struct {
	CN  []StatelessTopologyRuleItem `json:"cn,omitempty"`
	CDC []StatelessTopologyRuleItem `json:"cdc,omitempty"`
	GMS *XStoreTopologyRule         `json:"gms,omitempty"`
	DN  *XStoreTopologyRule         `json:"dn,omitempty"`
}

type TopologyRules struct {
	Selectors  []NodeSelectorItem     `json:"selectors,omitempty"`
	Components TopologyRuleComponents `json:"components,omitempty"`
}

type XStoreTemplate struct {
	// Engine of xstore. Default is operator dependent.
	// +optional
	Engine string `json:"engine,omitempty"`

	// Image for xstore. Should be replaced by default value if not present.
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullPolicy describes a policy for if/when to pull a container image (especially
	// for the engine container).
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets represents the secrets for pulling private images.
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// +kubebuilder:default=ClusterIP

	// Service type for xstore's service. Useful when needs an external IP.
	// Default is ClusterIP.
	// +optional
	ServiceType corev1.ServiceType `json:"serviceType,omitempty"`

	// +kubebuilder:default=true

	// HostNetwork mode.
	// +optional
	HostNetwork *bool `json:"hostNetwork,omitempty"`

	// DiskQuota for xstore.
	DiskQuota *resource.Quantity `json:"diskQuota,omitempty"`

	// +kubebuilder:default={limits: {cpu: 4, memory: "8Gi"}}

	// Resources. Default is limits of 4 cpu and 8Gi memory.
	Resources common.ExtendedResourceRequirements `json:"resources,omitempty"`
}

type CNTemplate struct {
	// Image for CN. Should be replaced by default value if not present.
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullPolicy describes a policy for if/when to pull a container image (especially
	// for the engine container).
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets represents the secrets for pulling private images.
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// HostNetwork mode.
	// +optional
	HostNetwork bool `json:"hostNetwork,omitempty"`

	// +kubebuilder:default={limits: {cpu: 4, memory: "8Gi"}}

	// Resources. Default is limits of 4 cpu and 8Gi memory.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

type CDCTemplate struct {
	// Image for CDC. Should be replaced by default value if not present.
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullPolicy describes a policy for if/when to pull a container image (especially
	// for the engine container).
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets represents the secrets for pulling private images.
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// HostNetwork mode.
	// +optional
	HostNetwork bool `json:"hostNetwork,omitempty"`

	// +kubebuilder:default={limits: {cpu: 4, memory: "8Gi"}}

	// Resources. Default is limits of 4 cpu and 8Gi memory.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

type TopologyNodeGMS struct {
	// Template of GMS xstore. If not provided, the operator will use
	// the template for DN as template for GMS.
	Template *XStoreTemplate `json:"template,omitempty"`
}

type TopologyNodeDN struct {
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1

	Replicas int32 `json:"replicas,omitempty"`

	// +kubebuilder:default={hostNetwork: true, resources: {limits: {cpu: 4, memory: "8Gi"}}}

	Template XStoreTemplate `json:"template,omitempty"`
}

type TopologyNodeCN struct {
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=0

	Replicas int32 `json:"replicas,omitempty"`

	// +kubebuilder:default={resources: {limits: {cpu: 4, memory: "8Gi"}}}

	Template CNTemplate `json:"template,omitempty"`
}

type TopologyNodeCDC struct {
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=0

	Replicas int32 `json:"replicas,omitempty"`

	// +kubebuilder:default={resources: {limits: {cpu: 4, memory: "8Gi"}}}

	Template CDCTemplate `json:"template,omitempty"`
}

type TopologyNodes struct {
	GMS TopologyNodeGMS `json:"gms,omitempty"`

	// +kubebuilder:default={replicas:2,template:{resources:{limits:{cpu:4,memory:"8Gi"}}}}

	CN TopologyNodeCN `json:"cn,omitempty"`

	// +kubebuilder:default={replicas:2,template:{hostNetwork:true,resources:{limits:{cpu:4,memory:"8Gi"}}}}

	DN TopologyNodeDN `json:"dn,omitempty"`

	CDC *TopologyNodeCDC `json:"cdc,omitempty"`
}

type Topology struct {
	// Version defines the default image version of the all components. Default is empty.
	// If not provided, one should specify the images manually.
	// +optional
	Version string `json:"version,omitempty"`

	// Rules defines the topology rules of components of the cluster.
	Rules TopologyRules `json:"rules,omitempty"`

	// +kubebuilder:default={cn:{replicas:2,template:{resources:{limits:{cpu:4,memory:"8Gi"}}}},dn:{replicas:2,template:{hostNetwork:true,resources:{limits:{cpu:4,memory:"8Gi"}}}}}

	// Node defines the replicas and template of components of the cluster.
	Nodes TopologyNodes `json:"nodes,omitempty"`
}
