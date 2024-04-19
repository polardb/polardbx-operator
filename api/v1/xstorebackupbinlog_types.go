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
	"k8s.io/apimachinery/pkg/util/intstr"
)

type XStoreBackupBinlogSpec struct {
	// +kubebuilder:validation:Required
	XStoreName string `json:"xstoreName"`
	// +optional
	XStoreUid string `json:"xstoreUid"`

	// +kubebuilder:default=galaxy

	// Engine is the engine used by xstore. Default is "galaxy".
	// +optional
	Engine string `json:"engine,omitempty"`

	// +kubebuilder:default=168
	RemoteExpireLogHours intstr.IntOrString `json:"remoteExpireLogHours,omitempty"`
	// +kubebuilder:default=7
	LocalExpireLogHours intstr.IntOrString `json:"localExpireLogHours,omitempty"`
	// +kubebuilder:default=60
	MaxLocalBinlogCount uint64 `json:"maxLocalBinlogCount,omitempty"`
	// +kubebuilder:default=true
	PointInTimeRecover bool `json:"pointInTimeRecover,omitempty"`
	// StorageProvider defines the backend storage to store the backup files.
	StorageProvider polardbx.BackupStorageProvider `json:"storageProvider,omitempty"`
	// +kubebuilder:default=CRC32
	BinlogChecksum string `json:"binlogChecksum,omitempty"`
}

type XStoreBackupBinlogPhase string

const (
	XStoreBackupBinlogPhaseNew              XStoreBackupBinlogPhase = ""
	XStoreBackupBinlogPhaseRunning          XStoreBackupBinlogPhase = "running"
	XStoreBackupBinlogPhaseCheckExpiredFile XStoreBackupBinlogPhase = "checkExpiredFile"
	XStoreBackupBinlogPhaseDeleting         XStoreBackupBinlogPhase = "deleting"
)

type XStoreBackupBinlogStatus struct {
	//Phase represents the executing phase in the controller
	Phase XStoreBackupBinlogPhase `json:"phase,omitempty"`

	//CheckExpireFileLastTime represents a timestamp of checking expired files
	CheckExpireFileLastTime uint64 `json:"checkExpireFileLastTime,omitempty"`

	//LastDeletedFiles represent the files deleted recently
	LastDeletedFiles []string `json:"lastDeletedFiles,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=xsblog
// +kubebuilder:printcolumn:name="PHASE",type=string,JSONPath=`.status.phase`

type XStoreBackupBinlog struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   XStoreBackupBinlogSpec   `json:"spec,omitempty"`
	Status XStoreBackupBinlogStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type XStoreBackupBinlogList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []XStoreBackupBinlog `json:"items"`
}

func init() {
	SchemeBuilder.Register(&XStoreBackupBinlog{}, &XStoreBackupBinlogList{})
}
