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
	"k8s.io/apimachinery/pkg/util/intstr"
)

type PolarDBXBackupBinlogSpec struct {
	// +kubebuilder:validation:Required
	PxcName string `json:"pxcName"`
	// +optional
	PxcUid string `json:"pxcUid"`
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

type PolarDbXBinlogPodStatus struct {
	PodName string `json:"podName,omitempty"`
	Version int64  `json:"version,omitempty"`
	Host    string `json:"host,omitempty"`
	WorkDir string `json:"workDir,omitempty"`
}

type BackupBinlogPhase string

const (
	BackupBinlogPhaseNew              BackupBinlogPhase = ""
	BackupBinlogPhaseRunning          BackupBinlogPhase = "running"
	BackupBinlogPhaseCheckExpiredFile BackupBinlogPhase = "checkExpiredFile"
	BackupBinlogPhaseDeleting         BackupBinlogPhase = "deleting"
)

type PolarDBXBackupBinlogStatus struct {
	// ObservedGeneration represents the observed generation of PolarDBXBackupBinlogSpec.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	//Phase represents the executing phase in the controller
	Phase BackupBinlogPhase `json:"phase,omitempty"`

	//CheckExpireFileLastTime represents a timestamp of checking expired files
	CheckExpireFileLastTime uint64 `json:"checkExpireFileLastTime,omitempty"`

	//LastDeletedFiles represent the files deleted recently
	LastDeletedFiles []string `json:"lastDeletedFiles,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=pxcblog
// +kubebuilder:printcolumn:name="PHASE",type=string,JSONPath=`.status.phase`

type PolarDBXBackupBinlog struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PolarDBXBackupBinlogSpec   `json:"spec,omitempty"`
	Status PolarDBXBackupBinlogStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type PolarDBXBackupBinlogList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PolarDBXBackupBinlog `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PolarDBXBackupBinlog{}, &PolarDBXBackupBinlogList{})
}
