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

package helper

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
)

func IsTopologyOrStaticConfigChanges(polardbx *polardbxv1.PolarDBXCluster) bool {
	if polardbx.Status.ObservedGeneration == polardbx.Generation {
		return false
	}

	spec := &polardbx.Spec
	snapshot := polardbx.Status.SpecSnapshot
	if snapshot == nil {
		panic("never be nil")
	}

	staticConfigChanged := !equality.Semantic.DeepEqual(spec.Config, snapshot.Config)
	topologyChanged := !equality.Semantic.DeepEqual(&spec.Topology, &snapshot.Topology)

	return topologyChanged || staticConfigChanged
}

func GetEncodeKeySecretKeySelector(polardbx *polardbxv1.PolarDBXCluster) *corev1.SecretKeySelector {
	if polardbx.Spec.Security != nil {
		return polardbx.Spec.Security.EncodeKey
	}
	return nil
}

func IsTLSEnabled(polardbx *polardbxv1.PolarDBXCluster) bool {
	return polardbx.Spec.Security != nil &&
		polardbx.Spec.Security.TLS != nil &&
		(len(polardbx.Spec.Security.TLS.SecretName) > 0 ||
			polardbx.Spec.Security.TLS.GenerateSelfSigned)
}

func IsMonitorConfigChanged(monitor *polardbxv1.PolarDBXMonitor) bool {
	spec := &monitor.Spec
	specSnapshot := monitor.Status.MonitorSpecSnapshot

	monitorConfigChanged := !equality.Semantic.DeepEqual(spec, specSnapshot)

	return monitorConfigChanged
}

func IsParameterChanged(polardbxParameter *polardbxv1.PolarDBXParameter) bool {
	spec := &polardbxParameter.Spec
	snapshot := polardbxParameter.Status.ParameterSpecSnapshot
	if snapshot == nil {
		panic("never be nil")
	}

	var parameterChanges, clusterChanges, templateChanges bool

	if spec.NodeType.GMS == nil {
		// GMS not set
		parameterChanges = !(equality.Semantic.DeepEqual(spec.NodeType.CN, snapshot.NodeType.CN) &&
			equality.Semantic.DeepEqual(spec.NodeType.DN, snapshot.NodeType.DN))
	} else {
		parameterChanges = !equality.Semantic.DeepEqual(spec, snapshot)
	}

	clusterChanges = !equality.Semantic.DeepEqual(spec.ClusterName, snapshot.ClusterName)
	templateChanges = !equality.Semantic.DeepEqual(spec.TemplateName, snapshot.TemplateName)

	return parameterChanges || clusterChanges || templateChanges
}
