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
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type XStoreFollowerSpec struct {
	// +kubebuilder:default=false

	//build the FromPod locally
	// +optional
	Local bool `json:"local,omitempty"`

	// +kubebuilder:default="follower"
	// +kubebuilder:validation:Enum=learner;logger;follower

	Role polardbxv1xstore.FollowerRole `json:"role,omitempty"`

	//NodeName is the dest Node to build the follower on
	// +optional
	NodeName string `json:"nodeName,omitempty"`

	//FromPodName represents the name of the pod which is used as the backup source
	// +optional
	FromPodName string `json:"fromPodName,omitempty"`

	//PodTemplate represents the configuration of pod which affect the resource schedule.
	// +optional
	TargetPodName string `json:"targetPodName,omitempty"`

	// +kubebuilder:validation:Required

	//XStoreName represents the name of xstore which the follower belongs to
	XStoreName string `json:"xStoreName,omitempty"`
}

type FlowFlagType struct {
	Name  string `json:"name,omitempty"`
	Value bool   `json:"value,omitempty"`
}

type XStoreFollowerStatus struct {
	//Phase represents the running phase of the task
	Phase polardbxv1xstore.FollowerPhase `json:"phase,omitempty"`

	//Message show some message about the current step
	Message string `json:"message,omitempty"`

	//BackupJobName represents the name of the back job
	BackupJobName string `json:"backupJobName,omitempty"`

	//RestoreJobName represents the name of the restore job
	RestoreJobName string `json:"restoreJobName,omitempty"`

	//CurrentJobName represents the name of the current job
	CurrentJobName string `json:"currentJobName,omitempty"`

	//CurrentJobTask represents the task name of the current job
	CurrentJobTask string `json:"currentJobTask,omitempty"`

	//TargetPodName represents the target pod name
	TargetPodName string `json:"targetPodName,omitempty"`

	//RebuildPodName represents the temporary pod name.
	RebuildPodName string `json:"rebuildPodName,omitempty"`

	//ToCleanHostPathVolume represents the host path volume that needs to be clean
	ToCleanHostPathVolume *polardbxv1xstore.HostPathVolume `json:"toCleanHostPathVolume,omitempty"`

	//RebuildNodeName represent the new node name of the pod
	RebuildNodeName string `json:"targetNodeName,omitempty"`

	//FlowFlags represent flow flags
	FlowFlags []FlowFlagType `json:"flowFlags,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=xf
// +kubebuilder:printcolumn:name="PHASE",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="MESSAGE",type=string,JSONPath=`.status.message`

// XStoreFollower is used create a learner node of the xstore
type XStoreFollower struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              XStoreFollowerSpec   `json:"spec,omitempty"`
	Status            XStoreFollowerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// XStoreFollowerList contains a list of XStoreFollower.
type XStoreFollowerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []XStoreFollower `json:"items"`
}

func init() {
	SchemeBuilder.Register(&XStoreFollower{}, &XStoreFollowerList{})
}
