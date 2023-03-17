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

type PolarDBXBackupScheduleSpec struct {
	// Schedule represents backup schedule in format of cron expression.
	Schedule string `json:"schedule,omitempty"`

	// Suspend denotes whether current schedule is paused.
	Suspend bool `json:"suspend,omitempty"`

	// +kubebuilder:default=0

	// MaxBackupCount defines limit of reserved backup.
	// If backup exceeds the limit, the eldest backup sets will be purged. Default is zero, which means no limit.
	// +optional
	MaxBackupCount int `json:"maxBackupCount,omitempty"`

	// BackupSpec defines spec of each backup.
	BackupSpec PolarDBXBackupSpec `json:"backupSpec,omitempty"`
}

type PolarDBXBackupScheduleStatus struct {
	// LastBackupTime records the time of the last backup.
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`

	// NextBackupTime records the scheduled time of the next backup.
	NextBackupTime *metav1.Time `json:"nextBackupTime,omitempty"`

	// LastBackup records the name of the last backup.
	LastBackup string `json:"lastBackup,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=pxcbackupschedule;pbs
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="SCHEDULE",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="LAST_BACKUP_TIME",type=string,JSONPath=`.status.lastBackupTime`
// +kubebuilder:printcolumn:name="NEXT_BACKUP_TIME",type=string,JSONPath=`.status.nextBackupTime`
// +kubebuilder:printcolumn:name="LAST_BACKUP",type=string,JSONPath=`.status.lastBackup`

type PolarDBXBackupSchedule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PolarDBXBackupScheduleSpec   `json:"spec,omitempty"`
	Status PolarDBXBackupScheduleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PolarDBXBackupScheduleList contains a list of PolarDBXBackupSchedule.
type PolarDBXBackupScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PolarDBXBackupSchedule `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PolarDBXBackupSchedule{}, &PolarDBXBackupScheduleList{})
}
