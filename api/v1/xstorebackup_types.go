/*
Copyright 2021 Alibaba, Inc.

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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type XStoreReference struct {
	Name string    `json:"name,omitempty"`
	UID  types.UID `json:"uid,omitempty"`
}

// XStoreBackupSpec defines the desired state of XStoreBackup
type XStoreBackupSpec struct {
	// +kubebuilder:default=galaxy

	// Engine is the engine used by xstore. Default is "galaxy".
	// +optional
	Engine string `json:"engine,omitempty"`

	XStore XStoreReference `json:"xstore,omitempty"`

	Timezone string `json:"timezone,omitempty"`

	// RetentionTime defines how long will this backup set be kept
	RetentionTime metav1.Duration `json:"retentionTime,omitempty"`

	// StorageProvider defines backup storage configuration
	StorageProvider polardbx.BackupStorageProvider `json:"storageProvider,omitempty"`

	// +kubebuilder:default=follower
	// +kubebuilder:validation:Enum=leader;follower

	// PreferredBackupRole defines the role of node on which backup will happen
	// +optional
	PreferredBackupRole string `json:"preferredBackupRole,omitempty"`

	// +kubebuilder:default=Retain
	// +kubebuilder:validation:Enum=Retain;Delete;OnFailure

	// CleanPolicy defines the clean policy for remote backup files when object of XStoreBackup is deleted.
	// Default is Retain.
	// +optional
	CleanPolicy polardbx.CleanPolicyType `json:"cleanPolicy,omitempty"`
}

// XStoreBackupStatus defines the observed state of XStoreBackup
type XStoreBackupStatus struct {
	Phase       XStoreBackupPhase `json:"phase,omitempty"`
	StartTime   *metav1.Time      `json:"startTime,omitempty"`
	EndTime     *metav1.Time      `json:"endTime,omitempty"`
	TargetPod   string            `json:"targetPod,omitempty"`
	CommitIndex int64             `json:"commitIndex,omitempty"`
	// StorageName represents the kind of Storage
	StorageName polardbx.BackupStorage `json:"storageName,omitempty"`
	// BackupRootPath stores the root path of backup set
	BackupRootPath string `json:"backupRootPath,omitempty"`
	// BackupSetTimestamp records timestamp of last event included in tailored binlog
	BackupSetTimestamp *metav1.Time `json:"backupSetTimestamp,omitempty"`

	// Message includes human-readable message related to current status.
	// +optional
	Message string `json:"message,omitempty"`

	// XStoreSpecSnapshot records the snapshot of xstore spec
	// +optional
	XStoreSpecSnapshot *XStoreSpec `json:"xstoreSpecSnapshot,omitempty"`
}

type XStoreBackupPhase string

const (
	XStoreBackupNew         XStoreBackupPhase = ""
	XStoreFullBackuping     XStoreBackupPhase = "Backuping"
	XStoreBackupCollecting  XStoreBackupPhase = "Collecting"
	XStoreBinlogBackuping   XStoreBackupPhase = "Binloging"
	XStoreBinlogWaiting     XStoreBackupPhase = "Waiting"
	XStoreMetadataBackuping XStoreBackupPhase = "MetadataBackuping"
	XStoreBackupFinished    XStoreBackupPhase = "Finished"
	XStoreBackupDummy       XStoreBackupPhase = "Dummy"
	XStoreBackupDeleting    XStoreBackupPhase = "Deleting"
	XstoreBackupFailed      XStoreBackupPhase = "Failed"
)

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=xsbackup;xsbackups;xsb
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="XSTORE",type=string,JSONPath=`.spec.xstore.name`
// +kubebuilder:printcolumn:name="START",type=string,JSONPath=`.status.startTime`
// +kubebuilder:printcolumn:name="END",type=string,JSONPath=`.status.endTime`
// +kubebuilder:printcolumn:name="PHASE",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="RETENTION",type=string,priority=1,JSONPath=`.spec.retentionTime`
// +kubebuilder:printcolumn:name="AGE",type=date,JSONPath=`.metadata.creationTimestamp`

// XStoreBackup is the Schema for the XStorebackups API
type XStoreBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   XStoreBackupSpec   `json:"spec,omitempty"`
	Status XStoreBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// XStoreBackupList contains a list of XStoreBackup
type XStoreBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []XStoreBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&XStoreBackup{}, &XStoreBackupList{})
}
