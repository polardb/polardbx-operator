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

type RestoreSpec struct {
	//BackupSet defines the source of backup set
	BackupSet string `json:"backupset,omitempty"`
	// From defines the source information, either backup sets, snapshot or an running cluster.
	From PolarDBXRestoreFrom `json:"from,omitempty"`

	// Time defines the specified time of the restored data, in the format of 'yyyy-MM-dd HH:mm:ss'. Required.
	Time string `json:"time,omitempty"`

	// TimeZone defines the specified time zone of the restore time. Default is the location of current cluster.
	// +optional
	TimeZone string `json:"timezone,omitempty"`
}

// PolarDBXRestoreFrom defines the source information of the restored cluster.
type PolarDBXRestoreFrom struct {
	// PolarBDXName defines the the polardbx name that this polardbx is restored from. Optional.
	// +optional
	PolarBDXName string `json:"clusterName,omitempty"`

	// BackupSelector defines the selector for the backups to be selected. Optional.
	// +optional
	BackupSelector map[string]string `json:"backupSelector,omitempty"`
}
