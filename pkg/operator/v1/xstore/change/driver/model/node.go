/*
Copyright 2022 Alibaba Group Holding Limited.

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

package model

import (
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	_map "github.com/alibaba/polardbx-operator/pkg/util/map"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
)

// PaxosVolume records the volume information of the paxos nodes.
type PaxosVolume struct {
	Name     string `json:"name,omitempty"`
	Host     string `json:"host,omitempty"`
	HostPath string `json:"host_path,omitempty"`
	Role     string `json:"role,omitempty"`
}

type PaxosInnerNode struct {
	Pod        string `json:"pod,omitempty"`
	Role       string `json:"role,omitempty"`
	Generation int64  `json:"generation,omitempty"`
	Set        string `json:"set,omitempty"`
	Index      int    `json:"index,omitempty"`
}

// PaxosNode records the node information of the paxos node.
type PaxosNode struct {
	PaxosInnerNode
	RebuildConfig map[string]interface{} `json:"config,omitempty"`
}

func (p *PaxosNode) DeepEquals(pNode *PaxosNode) bool {
	return p.PaxosInnerNode == pNode.PaxosInnerNode && _map.Equals(&p.RebuildConfig, &pNode.RebuildConfig)
}

type PaxosNodeStatus struct {
	PaxosNode  `json:",inline"`
	Host       string `json:"host,omitempty"`
	Volume     string `json:"volume,omitempty"`
	XStoreRole string `json:"xStoreRole,omitempty"`
	Ready      bool   `json:"status,omitempty"`
}

func (s *PaxosNodeStatus) FromPod(pod *corev1.Pod) error {
	s.Pod = pod.Name
	s.Role = strings.ToLower(pod.Labels[xstoremeta.LabelRole])
	s.Ready = k8shelper.IsPodReady(pod)
	var err error
	s.Generation, err = convention.GetGenerationLabelValue(pod)
	if err != nil {
		return err
	}
	s.Set = pod.Labels[xstoremeta.LabelNodeSet]
	s.Index, err = convention.PodIndexInNodeSet(pod.Name)
	if err != nil {
		return err
	}
	s.Host = pod.Spec.NodeName
	s.Volume = "" // TODO
	if s.RebuildConfig == nil {
		s.RebuildConfig = make(map[string]interface{})
	}
	if configHash, ok := pod.Labels[xstoremeta.LabelConfigHash]; ok {
		s.RebuildConfig[xstoremeta.LabelConfigHash] = configHash
	}
	s.RebuildConfig["NodeName"] = pod.Spec.NodeName
	if xStoreRole, ok := pod.Labels[xstoremeta.LabelRole]; ok {
		s.XStoreRole = xStoreRole
	}
	return nil
}

func FromPods(pods []corev1.Pod) ([]PaxosNodeStatus, error) {
	nodes := make([]PaxosNodeStatus, len(pods))
	for i, pod := range pods {
		if err := nodes[i].FromPod(&pod); err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

func ToMap(nodes []PaxosNodeStatus) map[string]PaxosNodeStatus {
	m := make(map[string]PaxosNodeStatus)
	for _, node := range nodes {
		m[node.Pod] = node
	}
	return m
}
