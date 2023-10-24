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

package context

import (
	"github.com/alibaba/polardbx-operator/api/v1/xstore"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/model"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/plan"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/factory"
)

// ExecutionContext is a serializable struct which provides the runtime execution information
// for the executors. It tracks all the history topologies in use and the index of the next step.
// It's large and should be cached in use and updated lazily.
type ExecutionContext struct {
	// Topologies in uses.
	Topologies map[int64]*xstore.Topology `json:"topologies,omitempty"`

	// Generation.
	Generation int64 `json:"generation,omitempty"`

	// Current running nodes.
	Running map[string]model.PaxosNodeStatus `json:"running,omitempty"`

	// Tracking nodes. This is the tracking set of the paxos node configuration.
	Tracking map[string]model.PaxosNodeStatus `json:"tracking,omitempty"`

	// Expected nodes.
	Expected map[string]model.PaxosNode `json:"expected,omitempty"`

	// Current usable volumes.
	Volumes map[string]model.PaxosVolume `json:"volumes,omitempty"`

	// Plan.
	Plan *plan.Plan `json:"plan,omitempty"`

	// StepIndex of the plan.
	StepIndex int `json:"step_index,omitempty"`

	PodFactory factory.ExtraPodFactory `json:"-"`
}

func (ec *ExecutionContext) GetNodeTemplate(generation int64, nodeSet string, index int) *polardbxv1xstore.NodeTemplate {
	topology := ec.Topologies[generation]
	if topology == nil {
		return nil
	}
	for _, ns := range topology.NodeSets {
		if ns.Name == nodeSet && index < int(ns.Replicas) {
			return factory.PatchNodeTemplate(
				topology.Template.DeepCopy(),
				ns.Template.DeepCopy(),
				true,
			)
		}
	}
	return nil
}

func (ec *ExecutionContext) SetPlan(plan plan.Plan) {
	ec.Plan = &plan
	ec.StepIndex = 0
}

func (ec *ExecutionContext) ensureTopologyMap() {
	if ec.Topologies == nil {
		ec.Topologies = make(map[int64]*polardbxv1xstore.Topology)
	}
}

func (ec *ExecutionContext) SetTopology(generation int64, topology *polardbxv1xstore.Topology) {
	if topology != nil {
		ec.ensureTopologyMap()
		ec.Topologies[generation] = topology.DeepCopy()
	}
}

func (ec *ExecutionContext) DeleteTopology(generation int64) {
	delete(ec.Topologies, generation)
}

func (ec *ExecutionContext) TopologyGenerations() []int64 {
	r := make([]int64, 0, len(ec.Topologies))
	for g := range ec.Topologies {
		r = append(r, g)
	}
	return r
}

func (ec *ExecutionContext) MarkStepIndex(index int) {
	if ec.StepIndex < index {
		ec.StepIndex = index
	}
}

func (ec *ExecutionContext) ContainsPaxosNode(name string) bool {
	_, ok := ec.Tracking[name]
	return ok
}

func (ec *ExecutionContext) AddPaxosNode(node model.PaxosNodeStatus) {
	ec.Tracking[node.Pod] = node
}

func (ec *ExecutionContext) RemovePaxosNode(name string) {
	delete(ec.Tracking, name)
}

func (ec *ExecutionContext) SetExtraPodFactory(extraPodFactory factory.ExtraPodFactory) {
	ec.PodFactory = extraPodFactory
}

func (ec *ExecutionContext) NeedRebuildPlan() bool {
	return ec.Plan == nil || !plan.CheckPlan(ec.Plan, ec.StepIndex, ec.Running, ec.Generation)
}

func (ec *ExecutionContext) Completed() bool {
	return ec.Plan != nil && ec.StepIndex >= len(ec.Plan.Steps)
}
