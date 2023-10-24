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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/alibaba/polardbx-operator/api/v1/polardbx"
)

type PolarDBXClusterSpec struct {
	// ShareGMS represents there's no standalone GMS instance but shares the first
	// DN as the GMS. It's not recommended in production but useful for tests in
	// environments with not so much CPU/memory resources.
	// Default is false.
	ShareGMS bool `json:"shareGMS,omitempty"`

	// +kubebuilder:validation:Enum=5;8;"5";"8";"5.7";"8.0"

	// ProtocolVersion represents the supported MySQL protocols provided by the cluster.
	// Must be either 5 (5.7) or 8 (8.0). Default is operator dependent.
	ProtocolVersion intstr.IntOrString `json:"protocolVersion,omitempty"`

	// ServiceName represents the name of main (access) service of the cluster.
	// It's set to the name of the cluster object when not provided.
	// +optional
	ServiceName string `json:"serviceName,omitempty"`

	// +kubebuilder:default=ClusterIP

	// ServiceType represents the service type of main (access) service of the cluster.
	// Default is ClusterIP.
	ServiceType corev1.ServiceType `json:"serviceType,omitempty"`

	// Topology defines the desired node topology and templates.
	Topology polardbx.Topology `json:"topology,omitempty"`

	// Config defines the configuration of the current cluster. Both dynamic and
	// static configs of CN and DN are included.
	Config polardbx.Config `json:"config,omitempty"`

	// Privileges defines the extra accounts that should be created while provisioning
	// the cluster. Specifying an item for the default root account will overwrite the
	// default random password with the given one.
	Privileges []polardbx.PrivilegeItem `json:"privileges,omitempty"`

	// Security defines the security config of the cluster, like SSL.
	// +optional
	Security *polardbx.Security `json:"security,omitempty"`

	// UpgradeStrategy defines the upgrade strategy for stateless nodes.
	UpgradeStrategy polardbx.UpgradeStrategyType `json:"upgradeStrategy,omitempty"`

	// Restore defines the restore specification. When provided, the operator
	// will create the cluster in restore mode. Restore might fail due to lack of
	// backups silently.
	// +optional
	Restore *polardbx.RestoreSpec `json:"restore,omitempty"`

	// ParameterTemplate defines the template of parameters used by cn/dn/gms.
	ParameterTemplate polardbx.ParameterTemplate `json:"parameterTemplate,omitempty"`

	// +kubebuilder:default=false

	// Readonly demonstrates whether the cluster is readonly. Default is false
	// +optional
	Readonly bool `json:"readonly,omitempty"`

	// PrimaryCluster is the name of primary cluster of the readonly cluster
	// +optional
	PrimaryCluster string `json:"primaryCluster,omitempty"`

	// InitReadonly is the list of readonly cluster that needs to be created and initialized
	// +optional
	InitReadonly []*polardbx.ReadonlyParam `json:"initReadonly,omitempty"`
}

type PolarDBXClusterStatus struct {
	// Phase is the current phase of the cluster.
	Phase polardbx.Phase `json:"phase,omitempty"`

	// Stage is the current stage of the xstore.
	Stage polardbx.Stage `json:"stage,omitempty"`

	// Conditions represent the current service state of the cluster.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions []polardbx.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the observed generation of the xstore spec.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Rand represents a random string value to avoid collision.
	Rand string `json:"randHash,omitempty"`

	// StatusForPrint represents the printable status of the cluster.
	StatusForPrint polardbx.StatusForPrint `json:"statusForPrint,omitempty"`

	// ReplicaStatus represents the replica status of the cluster.
	ReplicaStatus polardbx.ClusterReplicasStatus `json:"replicaStatus,omitempty"`

	// SpecSnapshot represents the snapshot of some aspects of the observed spec.
	// It should be updated atomically with the observed generation.
	SpecSnapshot *polardbx.SpecSnapshot `json:"specSnapshot,omitempty"`

	// Restarting represents if pods need restart
	Restarting bool `json:"restarting,omitempty"`

	// RestartingType represents the type of restart
	RestartingType string `json:"restartingType,omitempty"`

	// RestartingPods represents pods need to restart
	RestartingPods polardbx.RestartingPods `json:"restartingPods,omitempty"`

	//PitrStatus represents the status of the pitr restore
	PitrStatus *polardbx.PitrStatus `json:"pitrStatus,omitempty"`

	//LatestSyncReadonlyTs represents the lastest time sync readonly storage info to metadb
	ReadonlyStorageInfoHash string `json:"readonlyStorageInfoHash,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=px;pxc;polardbx
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="PROTOCOL",type=string,priority=1,JSONPath=`.spec.protocolVersion`
// +kubebuilder:printcolumn:name="GMS",type=string,JSONPath=`.status.statusForPrint.replicaStatus.gms`
// +kubebuilder:printcolumn:name="CN",type=string,JSONPath=`.status.statusForPrint.replicaStatus.cn`
// +kubebuilder:printcolumn:name="DN",type=string,JSONPath=`.status.statusForPrint.replicaStatus.dn`
// +kubebuilder:printcolumn:name="CDC",type=string,JSONPath=`.status.statusForPrint.replicaStatus.cdc`
// +kubebuilder:printcolumn:name="COLUMNAR",type=string,JSONPath=`.status.statusForPrint.replicaStatus.columnar`
// +kubebuilder:printcolumn:name="PHASE",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="DISK",type=string,JSONPath=`.status.statusForPrint.storageSize`
// +kubebuilder:printcolumn:name="STAGE",type=string,priority=1,JSONPath=`.status.stage`
// +kubebuilder:printcolumn:name="REBALANCE",type=string,priority=1,JSONPath=`.status.statusForPrint.rebalanceProcess`
// +kubebuilder:printcolumn:name="VERSION",type=string,priority=1,JSONPath=`.status.statusForPrint.detailedVersion`
// +kubebuilder:printcolumn:name="AGE",type=date,JSONPath=`.metadata.creationTimestamp`

type PolarDBXCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:default={topology:{nodes:{cn:{template:{resources:{limits:{cpu:4,memory:"8Gi"}}}},dn:{replicas:2,template:{hostNetwork:true,resources:{limits:{cpu:4,memory:"8Gi"}}}}}}}
	Spec   PolarDBXClusterSpec   `json:"spec,omitempty"`
	Status PolarDBXClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PolarDBXClusterList contains a list of PolarDBXCluster.
type PolarDBXClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PolarDBXCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PolarDBXCluster{}, &PolarDBXClusterList{})
}
