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
	// BackupSet defines the source of backup set.
	// It works only when PolarDBXBackup object of this BackupSet still exists.
	BackupSet string `json:"backupset,omitempty"`

	// StorageProvider defines storage used to perform backup
	// +optional
	StorageProvider *BackupStorageProvider `json:"storageProvider,omitempty"`

	// From defines the source information, either a running cluster, backup set path or backup selector.
	//
	// If PolarDBXRestoreFrom.BackupSetPath provided, restore will be performed using metadata backup in remote storage.
	// It works only when BackupSet is empty and StorageProvider is provided.
	From PolarDBXRestoreFrom `json:"from,omitempty"`

	// Time defines the specified time of the restored data, in the format of 'yyyy-MM-ddTHH:mm:ssZ'. Required.
	Time string `json:"time,omitempty"`

	// TimeZone defines the specified time zone of the restore time. Default is the location of current cluster.
	// +optional
	TimeZone string `json:"timezone,omitempty"`

	// +kubebuilder:default=false

	// SyncSpecWithOriginalCluster identifies whether restored cluster should use the same spec as the original cluster.
	// If the field is set to true, spec of original cluster is used; otherwise users have to declare spec manually or
	// use default spec, but replicas of dn will be forced to sync with original cluster now. Default is false
	// +optional
	SyncSpecWithOriginalCluster bool `json:"syncSpecWithOriginalCluster,omitempty"`

	// BinlogSource defines the binlog datasource
	// +optional
	BinlogSource *RestoreBinlogSource `json:"binlogSource,omitempty"`
}

// PolarDBXRestoreFrom defines the source information of the restored cluster.
type PolarDBXRestoreFrom struct {
	// PolarBDXName defines the polardbx name that this polardbx is restored from. Optional.
	// +optional
	PolarBDXName string `json:"clusterName,omitempty"`

	// BackupSetPath defines the location of backup set in remote storage
	BackupSetPath string `json:"backupSetPath,omitempty"`

	// BackupSelector defines the selector for the backups to be selected. Optional.
	// +optional
	BackupSelector map[string]string `json:"backupSelector,omitempty"`
}

// RestoreBinlogSource defines the binlog datasource
type RestoreBinlogSource struct {
	//Namespace defines the source binlog namespace
	Namespace string `json:"namespace,omitempty"`
	//Checksum defines the binlog file checksum.
	Checksum string `json:"checksum,omitempty"`
	//StorageProvider defines the source binlog sink
	StorageProvider *BackupStorageProvider `json:"storageProvider,omitempty"`
}
