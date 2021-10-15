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

	staticConfigChanged := !equality.Semantic.DeepEqual(spec.Config.CN.Static, snapshot.Config.CN.Static)
	topologyChanged := !equality.Semantic.DeepEqual(&spec.Topology, &snapshot.Topology)

	return topologyChanged || staticConfigChanged
}
