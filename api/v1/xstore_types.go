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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/alibaba/polardbx-operator/api/v1/xstore"
)

// XStoreRestoreFrom defines the source information of the restored cluster.
type XStoreRestoreFrom struct {

	// XStoreName defines the the xstore name that this xstore is restored from. Optional.
	// +optional
	XStoreName string `json:"clusterName,omitempty"`

	// BackupSelector defines the selector for the backups to be selected. Optional.
	// +optional
	BackupSelector map[string]string `json:"backupSelector,omitempty"`

	// BackupSetPath defines the location of backup set in remote storage
	BackupSetPath string `json:"backupSetPath,omitempty"`
}

// XStoreRestoreSpec defines the specification for restore a xstore with desired state.
type XStoreRestoreSpec struct {
	//BackupSet defines the source of backup set
	BackupSet string `json:"backupset,omitempty"`

	// From defines the source information, either backup sets, snapshot or an running cluster.
	From XStoreRestoreFrom `json:"from,omitempty"`

	// StorageProvider defines storage used to perform backup
	// +optional
	StorageProvider *polardbx.BackupStorageProvider `json:"storageProvider,omitempty"`

	// Time defines the specified time of the restored data, in the format of 'yyyy-MM-dd HH:mm:ss'. Required.
	Time string `json:"time,omitempty"`

	// TimeZone defines the specified time zone of the restore time. Default is the location of current cluster.
	// +optional
	TimeZone string `json:"timezone,omitempty"`

	PitrEndpoint string `json:"pitrEndpoiint,omitempty"`

	// BinlogSource defines the binlog datasource
	// +optional
	BinlogSource *RestoreBinlogSource `json:"binlogSource,omitempty"`
}

type TDE struct {
	// +kubebuilder:default=false
	// +optional
	Enable bool `json:"enable,omitempty"`

	// +kubebuilder:default=/data/mysql/mysql-keyring/keyring
	// +optional
	KeyringPath string `json:"keyringPath,omitempty"`
}

// RestoreBinlogSource defines the binlog datasource
type RestoreBinlogSource struct {
	//Namespace defines the source binlog namespace
	Namespace string `json:"namespace,omitempty"`
	//Checksum defines the binlog file checksum.
	Checksum string `json:"checksum,omitempty"`
	//StorageProvider defines the source binlog sink
	StorageProvider *polardbx.BackupStorageProvider `json:"storageProvider,omitempty"`
}

type XStoreSpec struct {
	// +kubebuilder:default=galaxy

	// Engine is the engine used by xstore. Default is "galaxy".
	// +optional
	Engine string `json:"engine,omitempty"`

	// ServiceName represents the service name of the xstore. Default is the same as the name.
	// +optional
	ServiceName string `json:"serviceName,omitempty"`

	// +kubebuilder:default="NodePort"

	// ServiceType represents the default service type of the xstore. Default is NodePort.
	// +optional
	ServiceType corev1.ServiceType `json:"serviceType,omitempty"`

	// ServiceLabels define the extra service labels of the xstore.
	// +optional
	ServiceLabels map[string]string `json:"serviceLabels,omitempty"`

	// Privileges defines the accounts that will be created and maintained automatically by
	// the controller.
	// +optional
	Privileges []xstore.Privilege `json:"privileges,omitempty"`

	// Topology is the specification of topology of the xstore.
	Topology xstore.Topology `json:"topology,omitempty"`

	// Config is the config of the xstore.
	Config xstore.Config `json:"config,omitempty"`

	// +kubebuilder:default="BestEffort"
	// +kubebuilder:validation:Enum=Force;BestEffort

	// UpgradeStrategy is the strategy when upgrading xstore. Default is BestEffort.
	// +optional
	UpgradeStrategy xstore.UpgradeStrategy `json:"upgradeStrategy,omitempty"`

	// ParameterTemplate defines the template of parameters used by cn/dn/gms.
	ParameterTemplate xstore.ParameterTemplate `json:"parameterTemplate,omitempty"`

	// +kubebuilder:default=false

	// Readonly demonstrates whether the xstore is readonly. Default is false
	// +optional
	Readonly bool `json:"readonly,omitempty"`

	// PrimaryCluster is the name of primary cluster of the readonly cluster
	// +optional
	PrimaryCluster string `json:"primaryCluster,omitempty"`

	// PrimaryXStore is the name of primary XStore of the readonly xstore
	// +optional
	PrimaryXStore string `json:"primaryXStore,omitempty"`

	// Restore defines the spec of restore.
	// +optional
	Restore *XStoreRestoreSpec `json:"restore,omitempty"`

	// TDE defines the transparent data encryption of clusters
	// +optional
	TDE TDE `json:"tde,omitempty"`

	// Exclusive if true, it means more resource isolation.
	// +optional
	Exclusive bool `json:"exclusive,omitempty"`

	// Tolerations specifies the tolerations of the Pods of the xstore.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

type XStoreStatus struct {
	// Phase is the current phase of the xstore.
	Phase xstore.Phase `json:"phase,omitempty"`

	// Stage is the current stage in phase of the xstore.
	Stage xstore.Stage `json:"stage,omitempty"`

	// Conditions represents the current service state of xstore.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions []xstore.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the observed generation of the xstore spec.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LeaderPod represents the pod name of the leader.
	LeaderPod string `json:"leaderPod,omitempty"`

	// ReadyPods represents the number of ready pods.
	ReadyPods int32 `json:"readyPods,omitempty"`

	// TotalPods represents the total number of pods.
	TotalPods int32 `json:"totalPods,omitempty"`

	// ReadyStatus represents the status of ready pods.
	ReadyStatus string `json:"readyStatus,omitempty"`

	// BoundVolumes represents the volumes used by this xstore.
	BoundVolumes map[string]*xstore.HostPathVolume `json:"boundVolumes,omitempty"`

	// LastVolumeSizeUpdateTime represents the last time that volumes' sizes updated.
	LastVolumeSizeUpdateTime *metav1.Time `json:"lastVolumeSizeUpdateTime,omitempty"`

	// TotalDataDirSize represents the total size of data dirs over all nodes.
	TotalDataDirSize string `json:"totalDataDirSize,omitempty"`

	// ObservedTopology records the snapshot of topology.
	ObservedTopology *xstore.Topology `json:"observedTopology,omitempty"`

	// ObservedConfig records the snapshot of mycnf.overlay
	ObservedConfig *xstore.Config `json:"observedConfig,omitempty"`

	// LastLogPurgeTime represents the last binlog/redo log purged time
	LastLogPurgeTime *metav1.Time `json:"lastLogPurgeTime,omitempty"`

	// Rand represents a random string value to avoid collision.
	Rand string `json:"randHash,omitempty"`

	// PodPorts represents the ports allocated (for host network)
	PodPorts map[string]xstore.PodPorts `json:"podPorts,omitempty"`

	// EngineVersion records the engine's version.
	EngineVersion string `json:"engineVersion,omitempty"`

	// ParameterName
	ParameterName string `json:"parameterName,omitempty"`

	// Restarting represents if pods need restart
	Restarting bool `json:"restarting,omitempty"`

	// Restarting represents pods restarting type
	RestartingType string `json:"restartingType,omitempty"`

	// UpdateConfigMap represents update cm.cnf.override in config map
	UpdateConfigMap bool `json:"updateConfigMap,omitempty"`

	// RestartingPods represents pods need to restart
	RestartingPods xstore.RestartingPods `json:"restartingPods,omitempty"`

	//PitrStatus represents the status of the pitr restore
	PitrStatus *xstore.PitrStatus `json:"pitrStatus,omitempty"`

	// +kubebuilder:default=false
	// TdeStatus represents if tde open
	TdeStatus bool `json:"tdeStatus,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=xs
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="ENGINE",type=string,priority=1,JSONPath=`.spec.engine`
// +kubebuilder:printcolumn:name="LEADER",type=string,JSONPath=`.status.leaderPod`
// +kubebuilder:printcolumn:name="READY",type=string,JSONPath=`.status.readyStatus`
// +kubebuilder:printcolumn:name="PHASE",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="STAGE",type=string,priority=1,JSONPath=`.status.stage`
// +kubebuilder:printcolumn:name="DISK",type=string,JSONPath=`.status.totalDataDirSize`
// +kubebuilder:printcolumn:name="VERSION",type=string,JSONPath=`.status.engineVersion`
// +kubebuilder:printcolumn:name="AGE",type=date,JSONPath=`.metadata.creationTimestamp`

// XStore is the schema for the xstore.
type XStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   XStoreSpec   `json:"spec,omitempty"`
	Status XStoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// XStoreList contains a list of xstore object.
type XStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []XStore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&XStore{}, &XStoreList{})
}
