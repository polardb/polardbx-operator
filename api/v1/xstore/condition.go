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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ConditionType string

// Valid xstore condition types.
const (
	// PodsReady indicates whether all pods in the cluster are ready.
	PodsReady ConditionType = "PodsReady"

	// LeaderReady indicates whether leader is elected in the cluster and ready for requests.
	LeaderReady ConditionType = "LeaderReady"

	// BinlogPurged indicates whether older binlog files are purged.
	BinlogPurged ConditionType = "BinlogPurged"

	// Restorable indicates whether the cluster is restorable.
	Restorable ConditionType = "Restorable"
)

type Condition struct {
	// Type is the type of the condition
	Type ConditionType `json:"type"`

	// Status is the status of the condition
	Status corev1.ConditionStatus `json:"status"`

	// Last time we probed the condition.
	// +optional
	LastProbeTime *metav1.Time `json:"lastProbeTime,omitempty"`

	// Last time the condition transition from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`

	// Unique, one-word, CamelCase reason for the condition's last transition.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Human-readable message indicating details about last transition.
	// +optional
	Message string `json:"message,omitempty"`
}

// PitrStatus represents the status ot pitr restore
type PitrStatus struct {
	PrepareJobEndpoint string `json:"prepareJobEndpoint,omitempty"`
	Job                string `json:"job,omitempty"`
}
