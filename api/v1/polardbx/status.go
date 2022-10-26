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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Phase defines the operating phase of the cluster.
type Phase string

// Valid phases.
const (
	PhaseNew        Phase = ""
	PhasePending    Phase = "Pending"
	PhaseCreating   Phase = "Creating"
	PhaseRunning    Phase = "Running"
	PhaseLocked     Phase = "Locked"
	PhaseUpgrading  Phase = "Upgrading"
	PhaseRestoring  Phase = "Restoring"
	PhaseDeleting   Phase = "Deleting"
	PhaseFailed     Phase = "Failed"
	PhaseRestarting Phase = "Restarting"
	PhaseUnknown    Phase = "Unknown"
)

// Stage defines the operating stage of the cluster.
// Different phases may have different stages.
type Stage string

// Valid stages.
const (
	StageEmpty          Stage = ""
	StageRebalanceStart Stage = "RebalanceStart"
	StageRebalanceWatch Stage = "RebalanceWatch"
	StageClean          Stage = "Clean"
)

// ConditionType represents the condition type of the cluster.
type ConditionType string

// Valid condition types.
const (
	GmsReady     ConditionType = "GmsReady"
	CnsReady     ConditionType = "CnsReady"
	DnsReady     ConditionType = "DnsReady"
	CdcReady     ConditionType = "CdcReady"
	ClusterReady ConditionType = "ClusterReady"
)

// Condition defines the condition and its status.
type Condition struct {
	// Type is the type of the condition.
	Type ConditionType `json:"type"`

	// Status is the status of the condition.
	Status corev1.ConditionStatus `json:"status"`

	// Last time we probed the condition.
	// +optional
	LastProbeTime *metav1.Time `json:"lastProbeTime,omitempty"`

	// Last time the condition transition from on status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`

	// Unique, one-word, CamelCase reason for the condition's last transition.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Human-readable message indicating details about last transition.
	// +optional
	Message string `json:"message,omitempty"`
}

// ReplicaStatusForPrint represents the printable status for replica status of nodes.
type ReplicaStatusForPrint struct {
	// GMS status, e.g. 1/1
	GMS string `json:"gms,omitempty"`
	// CN status, e.g. 1/2
	CN string `json:"cn,omitempty"`
	// DN status, e.g. 1/2
	DN string `json:"dn,omitempty"`
	// CDC status, e.g. 1/2 or - when CDC nodes not requested.
	CDC string `json:"cdc,omitempty"`
}

// StatusForPrint represents the overall printable status of the cluster.
type StatusForPrint struct {
	// ReplicaStatus for the status of nodes' replicas.
	ReplicaStatus ReplicaStatusForPrint `json:"replicaStatus,omitempty"`

	// RebalanceProgress for the status of data rebalance tasks.
	RebalanceProcess string `json:"rebalanceProcess,omitempty"`

	// DetailedVersion for the status of current detailed vesrion of the cluster,
	// which is dynamically acquired when cluster is ready.
	DetailedVersion string `json:"detailedVersion,omitempty"`

	// StorageSize represents the total storage size that used by this cluster.
	// The value is in IEC format and is simply gotten by accumulating the GMS'
	// and DNs' storage sizes.
	StorageSize string `json:"storageSize,omitempty"`

	// StorageSizeUpdateTime represents the last time that storage size is updated.
	// It's used to control the frequency of the updating progress.
	StorageSizeUpdateTime *metav1.Time `json:"storageSizeUpdateTime,omitempty"`
}

// ReplicasStatus represents the replica status of replicas of some nodes.
type ReplicasStatus struct {
	// Total defines the total size of the replica.
	Total int32 `json:"total,omitempty"`

	// Available represents how many replicas are currently available.
	Available int32 `json:"available,omitempty"`
}

func (rs *ReplicasStatus) Display() string {
	if rs == nil {
		return " - "
	}
	return fmt.Sprintf("%d/%d", rs.Available, rs.Total)
}

// ClusterReplicasStatus is a detailed replicas status of the cluster.
type ClusterReplicasStatus struct {
	// GMS defines the replica status for GMS.
	GMS ReplicasStatus `json:"gms,omitempty"`

	// CN defines the replica status for CN.
	CN *ReplicasStatus `json:"cn,omitempty"`

	// DN defines the replica status for DN.
	DN ReplicasStatus `json:"dn,omitempty"`

	// CDC defines the replica status for CDC.
	CDC *ReplicasStatus `json:"cdc,omitempty"`
}

type MonitorStatus string

const (
	MonitorStatusPending    MonitorStatus = ""
	MonitorStatusCreating   MonitorStatus = "Creating"
	MonitorStatusMonitoring MonitorStatus = "Monitoring"
	MonitorStatusUpdating   MonitorStatus = "Updating"
)

type ParameterPhase string

const (
	ParameterStatusNew       ParameterPhase = ""
	ParameterStatusCreating  ParameterPhase = "Creating"
	ParameterStatusRunning   ParameterPhase = "Running"
	ParameterStatusModifying ParameterPhase = "Modifying"
)
