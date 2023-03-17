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

package planner

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/context"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/model"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/plan"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/util"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	maputil "github.com/alibaba/polardbx-operator/pkg/util/map"
)

type Planner struct {
	selfHeal          bool
	rc                *xstorev1reconcile.Context
	ec                *context.ExecutionContext
	xstore            *polardbxv1.XStore
	pods              []corev1.Pod
	generation        int64
	generationChanged bool
	expectedNodes     map[string]model.PaxosNode
	runningNodes      map[string]model.PaxosNodeStatus
}

func (p *Planner) loadObjects() error {
	var err error

	if p.xstore, err = p.rc.GetXStore(); err != nil {
		return err
	}
	if p.selfHeal {
		p.generation = p.xstore.Status.ObservedGeneration
	} else {
		p.generation = p.xstore.Generation
	}

	if p.pods, err = p.rc.GetXStorePods(); err != nil {
		return err
	}

	return nil
}

func (p *Planner) runningGenerations() (map[int64]int, error) {
	generations := make(map[int64]int)
	for _, pod := range p.pods {
		g, err := xstoreconvention.GetGenerationLabelValue(&pod)
		if err != nil {
			return nil, fmt.Errorf("error retrieving generation label for %s: %w", pod.Name, err)
		}
		generations[g] = 1
	}
	return generations, nil
}

func (p *Planner) buildExpectedNodes() map[string]model.PaxosNode {
	topology := &p.xstore.Spec.Topology
	generation := p.xstore.Generation
	if p.selfHeal {
		topology = p.xstore.Status.ObservedTopology
		generation = p.xstore.Status.ObservedGeneration
	}

	nodes := make(map[string]model.PaxosNode)
	for _, ns := range topology.NodeSets {
		for i := 0; i < int(ns.Replicas); i++ {
			name := xstoreconvention.NewPodName(p.xstore, &ns, i)
			nodeRebuildConfig := p.createNodeRebuildConfig(name)
			nodes[name] = model.PaxosNode{
				PaxosInnerNode: model.PaxosInnerNode{
					Pod:        name,
					Role:       strings.ToLower(string(ns.Role)),
					Generation: generation,
					Set:        ns.Name,
					Index:      i,
				},
				RebuildConfig: nodeRebuildConfig,
			}
		}
	}

	return nodes
}

func (p *Planner) createNodeRebuildConfig(podName string) map[string]interface{} {
	rebuildConfig := make(map[string]interface{})
	config := xstoremeta.RebuildConfig{
		LogSeparation: strconv.FormatBool(p.xstore.Spec.Config.Dynamic.LogDataSeparation),
		NodeName:      p.xstore.Status.BoundVolumes[podName].Host,
	}
	rebuildConfig[xstoremeta.LabelConfigHash] = config.ComputeHash()
	return rebuildConfig
}

func podVolume(pod *corev1.Pod) string {
	// FIXME extract volume here.
	return ""
}

func (p *Planner) buildRunningNodes() (map[string]model.PaxosNodeStatus, error) {
	nodes := make(map[string]model.PaxosNodeStatus)

	for i := range p.pods {
		pod := &p.pods[i]
		generation, err := xstoreconvention.GetGenerationLabelValue(pod)
		if err != nil {
			return nil, err
		}
		index, err := xstoreconvention.PodIndexInNodeSet(pod.Name)
		if err != nil {
			return nil, err
		}
		nodes[pod.Name] = model.PaxosNodeStatus{
			PaxosNode: model.PaxosNode{
				PaxosInnerNode: model.PaxosInnerNode{
					Pod:        pod.Name,
					Role:       pod.Labels[xstoremeta.LabelNodeRole],
					Generation: generation,
					Set:        pod.Labels[xstoremeta.LabelNodeSet],
					Index:      index,
				},
				RebuildConfig: map[string]interface{}{
					xstoremeta.LabelConfigHash: pod.Labels[xstoremeta.LabelConfigHash],
				},
			},
			Host:       pod.Spec.NodeName,
			Volume:     podVolume(pod),
			XStoreRole: pod.Labels[xstoremeta.LabelRole],
		}
	}

	return nodes, nil
}

func (p *Planner) prepareExecutionContext() error {
	// Set generation.
	if p.selfHeal {
		p.generationChanged = false
		p.ec.Generation = p.xstore.Status.ObservedGeneration
	} else {
		p.generationChanged = p.ec.Generation != p.generation
		p.ec.Generation = p.generation
	}

	// Setup topology of new generation and remove outdated topologies.
	topology := &p.xstore.Spec.Topology
	if p.selfHeal {
		topology = p.xstore.Status.ObservedTopology
	}
	p.ec.SetTopology(p.ec.Generation, topology)

	// Calculate and update the expected paxos nodes if necessary.
	p.expectedNodes = p.buildExpectedNodes()
	p.ec.Expected = p.expectedNodes

	// Refill the running.
	runningNodes, err := p.buildRunningNodes()
	p.runningNodes = runningNodes
	if err != nil {
		return err
	}
	p.ec.Running = p.runningNodes

	return nil
}

func (p *Planner) Prepare() error {
	if err := p.loadObjects(); err != nil {
		return err
	}

	if err := p.prepareExecutionContext(); err != nil {
		return err
	}

	return nil
}

func (p *Planner) buildForHeal() (plan.Plan, error) {
	// If there's no usable volumes, we can't do anything.
	if len(p.ec.Volumes) == 0 {
		return plan.Plan{}, errors.New("no usable paxos volumes")
	}

	pl := plan.NewPlan(p.runningNodes)

	// Use update or create to restore the pod.
	for _, expectNode := range p.expectedNodes {
		_, exists := p.runningNodes[expectNode.Pod]
		if !exists {
			pl.AppendStep(plan.Step{
				Type:             plan.StepTypeUpdate,
				OriginGeneration: expectNode.Generation,
				OriginHost:       p.ec.Volumes[expectNode.Pod].Host,
				TargetGeneration: expectNode.Generation,
				Target:           expectNode.Pod,
				TargetRole:       expectNode.Role,
				NodeSet:          expectNode.Set,
				Index:            expectNode.Index,
			})
		}
	}

	for _, runningNode := range p.runningNodes {
		if _, exists := p.expectedNodes[runningNode.Pod]; !exists {
			return pl, errors.New("unexpected running nodes found, should not happen")
		}
	}

	// Finish the plan.
	pl.Finish()

	return pl, nil
}

func (p *Planner) build() (plan.Plan, error) {
	// If there's no usable volumes, we can't do anything.
	if len(p.ec.Volumes) == 0 {
		return plan.Plan{}, errors.New("no usable paxos volumes")
	}

	pl := plan.NewPlan(p.runningNodes)

	var leaderStep *plan.Step
	// TODO(fix) volumes
	// Create, update or replace expected nodes.
	for _, expectNode := range p.expectedNodes {
		runningNode, exists := p.runningNodes[expectNode.Pod]
		if !exists {
			stepType := plan.StepTypeUpdate
			_, volumeExists := p.ec.Volumes[expectNode.Pod]
			// If volumes exists, does not create a new data replica on another host
			if !volumeExists {
				stepType = plan.StepTypeCreate
			}

			pl.AppendStep(plan.Step{
				Type:             stepType,
				TargetGeneration: expectNode.Generation,
				Target:           expectNode.Pod,
				TargetRole:       expectNode.Role,
				NodeSet:          expectNode.Set,
				Index:            expectNode.Index,
			})

		} else if runningNode.Generation != expectNode.Generation {
			topologyChanged := !util.AreNodeSetsEqualInPodTemplate(
				p.ec.GetNodeTemplate(runningNode.Generation, runningNode.Set, runningNode.Index),
				p.ec.GetNodeTemplate(expectNode.Generation, expectNode.Set, expectNode.Index),
			)
			rebuildPodConfigChanged := maputil.Equals(&expectNode.RebuildConfig, &runningNode.RebuildConfig)

			stepType := plan.StepTypeUpdate
			if !topologyChanged && !rebuildPodConfigChanged {
				stepType = plan.StepTypeBumpGen
			} else if expectNode.Role != runningNode.Role {
				stepType = plan.StepTypeReplace
			}

			if runningNode.XStoreRole == xstoremeta.RoleLeader && leaderStep == nil {
				leaderStep = &plan.Step{
					Type:             stepType,
					OriginGeneration: runningNode.Generation,
					OriginHost:       runningNode.Host,
					TargetGeneration: expectNode.Generation,
					Target:           expectNode.Pod,
					TargetRole:       expectNode.Role,
					NodeSet:          expectNode.Set,
					Index:            expectNode.Index,
				}
				continue
			}

			pl.AppendStep(plan.Step{
				Type:             stepType,
				OriginGeneration: runningNode.Generation,
				OriginHost:       runningNode.Host,
				TargetGeneration: expectNode.Generation,
				Target:           expectNode.Pod,
				TargetRole:       expectNode.Role,
				NodeSet:          expectNode.Set,
				Index:            expectNode.Index,
			})
		}
	}
	if leaderStep != nil {
		pl.AppendStep(*leaderStep)
	}

	// Delete unexpected running nodes.
	for _, runningNode := range p.runningNodes {
		if _, exists := p.expectedNodes[runningNode.Pod]; !exists {
			pl.AppendStep(plan.Step{
				Type:             plan.StepTypeDelete,
				OriginGeneration: runningNode.Generation,
				OriginHost:       runningNode.Host,
				Target:           runningNode.Pod,
			})
		}
	}

	// Finish the plan.
	pl.Finish()

	return pl, nil
}

func (p *Planner) Build() error {
	var newPlan plan.Plan
	var err error
	if p.selfHeal {
		newPlan, err = p.buildForHeal()
	} else {
		newPlan, err = p.build()
	}
	if err != nil {
		return err
	}
	p.ec.SetPlan(newPlan)
	return nil
}

func (p *Planner) NeedRebuild() bool {
	return p.ec.NeedRebuildPlan()
}

func NewPlanner(rc *xstorev1reconcile.Context, ec *context.ExecutionContext, selfHeal bool) *Planner {
	return &Planner{rc: rc, ec: ec, selfHeal: selfHeal}
}
