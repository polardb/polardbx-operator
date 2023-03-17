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
	"github.com/alibaba/polardbx-operator/api/v1/polardbx"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type PolarDBXClusterReference struct {
	Name string    `json:"name,omitempty"`
	UID  types.UID `json:"uid,omitempty"`
}

type CleanPolicyType string

const (
	// CleanPolicyRetain represents that the backup files will be retained when the cluster is deleted.
	CleanPolicyRetain CleanPolicyType = "Retain"

	// CleanPolicyDelete represents that the backup files will be deleted when the cluster is deleted.
	CleanPolicyDelete CleanPolicyType = "Delete"

	// CleanPolicyOnFailure represents that the backup object will be deleted when the backup is failed.
	CleanPolicyOnFailure CleanPolicyType = "OnFailure"
)

// PolarDBXBackupSpec defines the desired state of PolarDBXBackup
type PolarDBXBackupSpec struct {
	// Cluster represents the reference of target polardbx cluster to perform the backup action.
	Cluster PolarDBXClusterReference `json:"cluster,omitempty"`

	// RetentionTime defines the retention time of the backup. The format is the same
	// with metav1.Duration. Must be provided.
	RetentionTime metav1.Duration `json:"retentionTime,omitempty"`

	// +kubebuilder:default=Retain
	// +kubebuilder:validation:Enum=Retain;Delete;OnFailure

	// CleanPolicy defines the clean policy when cluster is deleted. Default is Retain.
	// +optional
	CleanPolicy CleanPolicyType `json:"cleanPolicy,omitempty"`

	// StorageProvider defines the backend storage to store the backup files.
	StorageProvider polardbx.BackupStorageProvider `json:"storageProvider,omitempty"`

	// +kubebuilder:default=follower
	// +kubebuilder:validation:Enum=leader;follower

	// PreferredBackupRole defines the role of node on which backup will happen
	// +optional
	PreferredBackupRole string `json:"preferredBackupRole,omitempty"`
}

// PolarDBXBackupPhase defines the phase of backup
type PolarDBXBackupPhase string

const (
	BackupNew         PolarDBXBackupPhase = ""
	FullBackuping     PolarDBXBackupPhase = "FullBackuping"
	BackupCollecting  PolarDBXBackupPhase = "Collecting"
	BackupCalculating PolarDBXBackupPhase = "Calculating"
	BinlogBackuping   PolarDBXBackupPhase = "BinlogBackuping"
	MetadataBackuping PolarDBXBackupPhase = "MetadataBackuping"
	BackupFinished    PolarDBXBackupPhase = "Finished"
	BackupFailed      PolarDBXBackupPhase = "Failed"
	BackupDummy       PolarDBXBackupPhase = "Dummy"
)

// PolarDBXBackupStatus defines the observed state of PolarDBXBackup
type PolarDBXBackupStatus struct {
	// StartTime represents the backup start time.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// EndTime represents the backup end time.
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`

	// Phase represents the backup phase.
	// +optional
	Phase PolarDBXBackupPhase `json:"phase,omitempty"`

	// Reason represents the reason of failure.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Backups represents the underlying backup objects of xstore. The key is
	// cluster name, and the value is the backup name.
	// +optional
	Backups map[string]string `json:"backups,omitempty"`

	// XStores represents the backup xstore name.
	// +optional
	XStores []string `json:"xstores,omitempty"`

	// ClusterSpecSnapshot records the snapshot of polardbx cluster spec
	// +optional
	ClusterSpecSnapshot *PolarDBXClusterSpec `json:"clusterSpecSnapshot,omitempty"`

	// HeartBeatName represents the heartbeat name of backup.
	HeartBeatName string `json:"heartbeat,omitempty"`

	// StorageName represents the kind of Storage
	StorageName polardbx.BackupStorage `json:"storageName,omitempty"`

	// BackupRootPath stores the root path of backup set
	BackupRootPath string `json:"backupRootPath,omitempty"`

	// BackupSetTimestamp records timestamp of last event included in tailored binlog per xstore
	BackupSetTimestamp map[string]*metav1.Time `json:"backupSetTimestamp,omitempty"`

	// LatestRecoverableTimestamp records the latest timestamp that can recover from current backup set
	LatestRecoverableTimestamp *metav1.Time `json:"latestRecoverableTimestamp,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=pxcbackup;pxcbackups;pxb
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="CLUSTER",type=string,JSONPath=`.spec.cluster.name`
// +kubebuilder:printcolumn:name="START",type=string,JSONPath=`.status.startTime`
// +kubebuilder:printcolumn:name="END",type=string,JSONPath=`.status.endTime`
// +kubebuilder:printcolumn:name="RESTORE_TIME",type=string,JSONPath=`.status.latestRecoverableTimestamp`
// +kubebuilder:printcolumn:name="PHASE",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="RETENTION",type=string,priority=1,JSONPath=`.spec.retentionTime`
// +kubebuilder:printcolumn:name="AGE",type=date,JSONPath=`.metadata.creationTimestamp`

// PolarDBXBackup is the Scheme for the polardbxbackups API
type PolarDBXBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PolarDBXBackupSpec   `json:"spec,omitempty"`
	Status PolarDBXBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PolarDBXBackupList contains a list of PolarDBXBackup
type PolarDBXBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PolarDBXBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PolarDBXBackup{}, &PolarDBXBackupList{})
}
