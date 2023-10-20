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
	"encoding/json"
	"fmt"
	"sort"

	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/model"
)

type StepType string

// Valid plan node types. The order of the raw values is
// the order of the node types.
const (
	StepTypeSnapshot StepType = "Snapshot"
	StepTypeBumpGen  StepType = "BumpGen"
	StepTypeUpdate   StepType = "Update"
	StepTypeReplace  StepType = "Replace"
	StepTypeCreate   StepType = "Create"
	StepTypeDelete   StepType = "Delete"
)

// Implementation of Stringer.
func (t StepType) String() string {
	return string(t)
}

// Step is the basic unit of a plan.
type Step struct {
	// Type of the step.
	Type StepType `json:"type,omitempty"`
	// Original generation of the target.
	// Valid for StepTypeBumpGen, StepTypeUpdate, StepTypeReplace and StepTypeDelete.
	OriginGeneration int64 `json:"origin_generation,omitempty"`
	// Original host of the target.
	// Valid for StepTypeBumpGen, StepTypeUpdate, StepTypeReplace and StepTypeDelete.
	OriginHost string `json:"origin_host,omitempty"`
	// Target generation of the target. Valid for all StepTypeBumpGen, StepTypeUpdate, StepTypeReplace and StepTypeCreate.
	TargetGeneration int64 `json:"target_generation,omitempty"`
	// Target host of the target. If it is empty, it means the host is not specified.
	// Valid for all StepTypeBumpGen, StepTypeUpdate, StepTypeReplace and StepTypeCreate.
	TargetHost string `json:"target_host,omitempty"`
	// Target pod of the step.
	Target string `json:"target,omitempty"`
	// Target role.
	TargetRole string `json:"role,omitempty"`
	// Node set.
	NodeSet string `json:"node_set,omitempty"`
	// Index in node set.
	Index int `json:"index,omitempty"`
	// Volumes that is going to use in the step.
	Volumes []string `json:"volumes,omitempty"`
}

func (step *Step) Priority() int {
	switch step.Type {
	case StepTypeBumpGen:
		return 0
	case StepTypeSnapshot:
		return 1
	case StepTypeUpdate:
		return 2
	case StepTypeReplace:
		return 3
	case StepTypeCreate:
		return 4
	case StepTypeDelete:
		return 5
	default:
		panic("undefined")
	}
}

func (step *Step) Description() string {
	switch step.Type {
	case StepTypeSnapshot:
		return "Snapshot"
	case StepTypeBumpGen:
		return fmt.Sprintf(
			"BumpGen(%s, from: %d, to: %d)",
			step.Target,
			step.OriginGeneration,
			step.TargetGeneration,
		)
	case StepTypeUpdate:
		return fmt.Sprintf("Update(%s, %d)", step.Target, step.TargetGeneration)
	case StepTypeReplace:
		return fmt.Sprintf("Replace(%s, %d)", step.Target, step.TargetGeneration)
	case StepTypeCreate:
		return fmt.Sprintf("Create(%s, %d)", step.Target, step.TargetGeneration)
	case StepTypeDelete:
		return fmt.Sprintf("Delete(%s, %d)", step.Target, step.OriginGeneration)
	default:
		panic("undefined")
	}
}

func (step *Step) String() string {
	c, _ := json.Marshal(step)
	return string(c)
}

// Plan is a sequence of steps that are going to be executed one by one.
type Plan struct {
	// Nodes observed when the plan is built.
	Nodes []model.PaxosNode `json:"nodes,omitempty"`

	// Steps of the plan.
	Steps []Step `json:"steps,omitempty"`
}

func NewPlan(runningNodes map[string]model.PaxosNodeStatus) Plan {
	nodes := make([]model.PaxosNode, 0, len(runningNodes))
	for _, n := range runningNodes {
		nodes = append(nodes, n.PaxosNode)
	}
	return Plan{Nodes: nodes}
}

func (p *Plan) AppendStep(s Step) {
	p.Steps = append(p.Steps, s)
}

func (p *Plan) Finish() {
	createOrReplaceFound := false
	for _, step := range p.Steps {
		if step.Type == StepTypeCreate || step.Type == StepTypeReplace {
			createOrReplaceFound = true
			break
		}
	}

	if createOrReplaceFound {
		p.AppendStep(Step{Type: StepTypeSnapshot})
	}

	sort.Slice(p.Steps, func(i, j int) bool {
		pi, pj := p.Steps[i].Priority(), p.Steps[j].Priority()
		if pi == pj {
			return p.Steps[i].Target < p.Steps[j].Target
		}
		return pi < pj
	})
}
