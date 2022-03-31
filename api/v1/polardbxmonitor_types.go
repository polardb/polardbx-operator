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
)

type PolarDBXMonitorSpec struct {
	ClusterName string `json:"clusterName,omitempty"`

	// +kubebuilder:default="30s"
	// MonitorInterval define the metrics scrape interval
	MonitorInterval metav1.Duration `json:"monitorInterval,omitempty"`

	// +kubebuilder:default="10s"
	// MonitorInterval define the metrics scrape interval
	ScrapeTimeout metav1.Duration `json:"scrapeTimeout,omitempty"`
}

type PolarDBXMonitorStatus struct {
	MonitorStatus polardbx.MonitorStatus `json:"monitorStatus,omitempty"`

	MonitorSpecSnapshot *PolarDBXMonitorSpec `json:"monitorSpecSnapshot,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=pxm;polardbxmonitor
// +kubebuilder:printcolumn:name="CLUSTER",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="INTERVAL",type=string,JSONPath=`.status.monitorSpecSnapshot.monitorInterval`
// +kubebuilder:printcolumn:name="STATUS",type=string,JSONPath=`.status.monitorStatus`
// +kubebuilder:printcolumn:name="AGE",type=date,JSONPath=`.metadata.creationTimestamp`
type PolarDBXMonitor struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PolarDBXMonitorSpec   `json:"spec,omitempty"`
	Status PolarDBXMonitorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PolarDBXClusterList contains a list of PolarDBXCluster.
type PolarDBXMonitorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PolarDBXMonitor `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PolarDBXMonitor{}, &PolarDBXMonitorList{})
}
