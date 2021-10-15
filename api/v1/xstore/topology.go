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
	v1 "k8s.io/api/core/v1"

	"github.com/alibaba/polardbx-operator/api/v1/common"
)

// NodeSpec defines the specification of a xstore node.
type NodeSpec struct {
	// Image is the image of xstore engine. Controller should fill a default image
	// if not specified.
	Image string `json:"image,omitempty"`

	// ImagePullSecrets represents the secrets for pulling private images.
	ImagePullSecrets []v1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// +kubebuilder:default=true

	// HostNetwork defines whether the node uses the host network. Default is true.
	HostNetwork *bool `json:"hostNetwork,omitempty"`

	// Affinity defines the affinity of the node (pod).
	// +optional
	Affinity *v1.Affinity `json:"affinity,omitempty"`

	// Resources is the requested resources of the node.
	Resources *common.ExtendedResourceRequirements `json:"resources,omitempty"`
}

// NodeTemplate defines the template of a xstore node.
type NodeTemplate struct {
	// ObjectMeta is the metadata template only contains labels and annotations.
	ObjectMeta common.PartialObjectMeta `json:"metadata,omitempty"`

	// Spec is the specification of xstore node.
	Spec NodeSpec `json:"spec,omitempty"`
}

// NodeSet defines a set of xstore nodes shares the same role and template.
type NodeSet struct {
	// Name is the name of this node set. Must be unique in specification.
	Name string `json:"name,omitempty"`

	// +kubebuilder:validation:Enum=Candidate;Voter;Learner

	// Role is the role of nodes defined by this node set.
	Role NodeRole `json:"role,omitempty"`

	// Replicas is the number of nodes in this node set.
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas,omitempty"`

	// Template defines the customized template of nodes in current node set. If not
	// specified, controller should use the global template instead. If partially
	// specified, the value used is merged with determined strategy.
	// +optional
	Template *NodeTemplate `json:"template,omitempty"`
}

// Topology defines the topology of xstore, consisting of a global node template
// and multiple node sets.
type Topology struct {
	// Template is the global node template of xstore.
	Template NodeTemplate `json:"template,omitempty"`

	// NodeSets is the total node sets of xstore.
	NodeSets []NodeSet `json:"nodeSets,omitempty"`
}
