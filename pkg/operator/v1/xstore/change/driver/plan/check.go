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

package plan

import (
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/model"
)

func copyNodes(nodes []model.PaxosNode) []model.PaxosNode {
	cp := make([]model.PaxosNode, len(nodes))
	copy(cp, nodes)
	return cp
}

func nodeSliceToMap(nodes []model.PaxosNode) map[string]model.PaxosNode {
	m := make(map[string]model.PaxosNode)
	for _, n := range nodes {
		m[n.Pod] = n
	}
	return m
}

func statusMapToNodeMap(status map[string]model.PaxosNodeStatus) map[string]model.PaxosNode {
	m := make(map[string]model.PaxosNode)
	for k, v := range status {
		m[k] = v.PaxosNode
	}
	return m
}

func (step *Step) applyInner(node *model.PaxosNode) *model.PaxosNode {
	if node != nil {
		*node = model.PaxosNode{
			PaxosInnerNode: model.PaxosInnerNode{
				Pod:        step.Target,
				Role:       step.TargetRole,
				Generation: step.TargetGeneration,
				Set:        step.NodeSet,
				Index:      step.Index,
			},
			RebuildConfig: map[string]interface{}{},
		}
	}

	return node
}

func (step *Step) apply(nodes map[string]model.PaxosNode) {
	switch step.Type {
	case StepTypeSnapshot:
		return
	case StepTypeBumpGen,
		StepTypeUpdate,
		StepTypeReplace:
		node := nodes[step.Target]
		nodes[step.Target] = *step.applyInner(&node)
	case StepTypeDelete:
		delete(nodes, step.Target)
	case StepTypeCreate:
		nodes[step.Target] = *step.applyInner(nil)
	default:
		panic("undefined")
	}
}

func matches(a, b map[string]model.PaxosNode) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		vb, ok := b[k]
		if !ok {
			return false
		}
		if !v.DeepEquals(&vb) {
			return false
		}
	}
	return true
}

// CheckPlan checks if current running nodes sticks to the plan.
func CheckPlan(plan *Plan, curStep int, running map[string]model.PaxosNodeStatus, generation int64) bool {
	runningNodes := statusMapToNodeMap(running)

	expectedNodes := nodeSliceToMap(plan.Nodes)

	// Simulate the executed steps.
	for i, s := range plan.Steps {
		if i < curStep {
			s.apply(expectedNodes)
		}
	}

	// Exclude the current step target.
	if curStep < len(plan.Steps) {
		if plan.Steps[curStep].TargetGeneration < generation {
			return false
		}
		delete(expectedNodes, plan.Steps[curStep].Target)
		delete(runningNodes, plan.Steps[curStep].Target)
	}

	return matches(expectedNodes, runningNodes)
}
