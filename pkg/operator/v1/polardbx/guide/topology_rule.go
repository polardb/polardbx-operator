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

package guide

import (
	"errors"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
)

func init() {
	registerHandler(polardbxmeta.AnnotationTopologyRuleGuide, &topologyRuleHandler{})
}

type TopologyRuleGuideMode string

const (
	RuleGuide1l2c = "zone-mode-1l2c"
	RuleGuide1l3c = "zone-mode-1l3c"
	RuleGuide2l3c = "zone-mode-2l3c"
	RuleGuide3l5c = "zone-mode-3l5c"
)

var ErrInvalidRuleGuide = errors.New("invalid rule guide")

type RegionZones struct {
	RegionLabel string
	Region      string
	ZoneLabel   string
	Zones       []string
}

type topologyRuleHandler struct{}

func (h *topologyRuleHandler) extractKeyValues(kv string, defaultKey string) (string, []string, error) {
	split := strings.Split(kv, "=")
	if len(split) > 2 {
		return "", nil, ErrInvalidRuleGuide
	} else if len(split) == 2 {
		return split[0], strings.Split(split[1], "|"), nil
	} else {
		return defaultKey, strings.Split(split[0], "|"), nil
	}
}

func (h *topologyRuleHandler) extractKeyValue(kv string, defaultKey string) (string, string, error) {
	split := strings.Split(kv, "=")
	if len(split) > 2 {
		return "", "", ErrInvalidRuleGuide
	} else if len(split) == 2 {
		return split[0], split[1], nil
	} else {
		return defaultKey, split[0], nil
	}
}

func (h *topologyRuleHandler) extractRegionZones(s string) (RegionZones, error) {
	split := strings.Split(s, ",")
	if len(split) < 1 || len(split) > 2 {
		return RegionZones{}, ErrInvalidRuleGuide
	}

	if len(split) == 1 {
		key, values, err := h.extractKeyValues(split[0], corev1.LabelTopologyZone)
		if err != nil {
			return RegionZones{}, err
		}
		return RegionZones{
			ZoneLabel: key,
			Zones:     values,
		}, nil
	} else {
		regionLabel, region, err := h.extractKeyValue(split[0], corev1.LabelTopologyRegion)
		if err != nil {
			return RegionZones{}, err
		}
		zoneLabel, zones, err := h.extractKeyValues(split[1], corev1.LabelTopologyZone)
		if err != nil {
			return RegionZones{}, err
		}
		return RegionZones{
			RegionLabel: regionLabel,
			Region:      region,
			ZoneLabel:   zoneLabel,
			Zones:       zones,
		}, nil
	}
}

func (h *topologyRuleHandler) extractRegionsAndZones(s string) ([]RegionZones, error) {
	s = strings.ReplaceAll(s, " ", "")

	regionAndZones := make([]RegionZones, 0, 1)
	leftParenIdx, rightParenIdx := 0, len(s)-1
	for leftParenIdx >= 0 {
		if s[leftParenIdx] != '(' {
			return nil, ErrInvalidRuleGuide
		}
		rightParenIdx = strings.IndexRune(s[leftParenIdx+1:], ')') + leftParenIdx + 1
		if rightParenIdx < 0 {
			return nil, ErrInvalidRuleGuide
		}
		rz, err := h.extractRegionZones(s[leftParenIdx+1 : rightParenIdx])
		if err != nil {
			return nil, err
		}
		regionAndZones = append(regionAndZones, rz)
		if rightParenIdx+1 < len(s) {
			leftParenIdx = rightParenIdx + 1
		} else {
			break
		}
	}

	return regionAndZones, nil
}

func (h *topologyRuleHandler) parseTopologyRuleGuide(s string) (TopologyRuleGuideMode, []RegionZones, error) {
	s = strings.TrimSpace(s)
	// Topology rule guide should in the format of
	// guide-mode({{region_label=}region}, {zone_label=}zone1{|zone2}){(...))
	firstLeftParen := strings.IndexRune(s, '(')
	if firstLeftParen < 0 {
		return "", nil, ErrInvalidRuleGuide
	}

	if s[len(s)-1] != ')' {
		return "", nil, ErrInvalidRuleGuide
	}

	mode := s[:firstLeftParen]
	regionsAndZones, err := h.extractRegionsAndZones(s[firstLeftParen:])
	if err != nil {
		return "", nil, err
	}

	switch mode {
	case RuleGuide1l2c:
		if len(regionsAndZones) != 1 ||
			len(regionsAndZones[0].Zones) != 2 {
			return "", nil, ErrInvalidRuleGuide
		}
		return TopologyRuleGuideMode(mode), regionsAndZones, nil
	case RuleGuide1l3c:
		if len(regionsAndZones) != 1 ||
			len(regionsAndZones[0].Zones) != 3 {
			return "", nil, ErrInvalidRuleGuide
		}
		return TopologyRuleGuideMode(mode), regionsAndZones, nil
	case RuleGuide2l3c:
		if len(regionsAndZones) != 2 ||
			len(regionsAndZones[0].Zones) != 2 ||
			len(regionsAndZones[1].Zones) != 1 {
			return "", nil, ErrInvalidRuleGuide
		}
		return TopologyRuleGuideMode(mode), regionsAndZones, nil
	case RuleGuide3l5c:
		if len(regionsAndZones) != 3 ||
			len(regionsAndZones[0].Zones) != 2 ||
			len(regionsAndZones[1].Zones) != 2 ||
			len(regionsAndZones[2].Zones) != 1 {
			return "", nil, ErrInvalidRuleGuide
		}
		return TopologyRuleGuideMode(mode), regionsAndZones, nil
	default:
		return "", nil, ErrInvalidRuleGuide
	}
}

func (h *topologyRuleHandler) cnReplicaScaleBase(mode TopologyRuleGuideMode) int {
	switch mode {
	case RuleGuide1l2c:
		return 2
	case RuleGuide1l3c:
		return 3
	case RuleGuide2l3c:
		return 5
	case RuleGuide3l5c:
		return 9
	default:
		return 1
	}
}

func (h *topologyRuleHandler) Handle(rc *polardbxv1reconcile.Context, obj *polardbxv1.PolarDBXCluster) error {
	ruleGuide := obj.Annotations[polardbxmeta.AnnotationTopologyRuleGuide]
	mode, regionAndZones, err := h.parseTopologyRuleGuide(ruleGuide)
	if err != nil {
		return err
	}

	factory := NewTopologyRuleFactory(rc)
	rules, err := factory.NewTopologyRules(mode, regionAndZones)
	if err != nil {
		return err
	}
	rules.DeepCopyInto(&obj.Spec.Topology.Rules)
	delete(obj.Annotations, polardbxmeta.AnnotationConfigGuide)

	// Adjust the replicas of CN
	scaleBase := h.cnReplicaScaleBase(mode)
	if int(obj.Spec.Topology.Nodes.CN.Replicas)%scaleBase != 0 {
		obj.Spec.Topology.Nodes.CN.Replicas /= int32(scaleBase)
		if obj.Spec.Topology.Nodes.CN.Replicas == 0 {
			obj.Spec.Topology.Nodes.CN.Replicas = int32(scaleBase)
		}
	}

	return nil
}

type TopologyRuleFactory interface {
	NewTopologyRules(TopologyRuleGuideMode, []RegionZones) (*polardbxv1polardbx.TopologyRules, error)
}

type topologyRuleFactory struct {
	rc *polardbxv1reconcile.Context
}

func (t *topologyRuleFactory) newNodeSelector(regionLabel, region, zoneLabel, zone string) corev1.NodeSelector {
	expressions := make([]corev1.NodeSelectorRequirement, 0, 1)
	if len(region) > 0 {
		expressions = append(expressions, corev1.NodeSelectorRequirement{
			Key:      regionLabel,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{region},
		})
	}

	expressions = append(expressions, corev1.NodeSelectorRequirement{
		Key:      zoneLabel,
		Operator: corev1.NodeSelectorOpIn,
		Values:   []string{zone},
	})

	return corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{
			{
				MatchExpressions: expressions,
			},
		},
	}
}

func (t *topologyRuleFactory) newNodeSelectorName(region, zone string) string {
	if len(region) > 0 {
		return region + "/" + zone
	}
	return zone
}

func (t *topologyRuleFactory) NewTopologyRules(mode TopologyRuleGuideMode, regionAndZones []RegionZones) (*polardbxv1polardbx.TopologyRules, error) {
	zoneCnt := 0
	for _, rz := range regionAndZones {
		zoneCnt += len(rz.Zones)
	}

	nodeSelectors := make([]polardbxv1polardbx.NodeSelectorItem, 0, zoneCnt)
	for _, rz := range regionAndZones {
		for _, zone := range rz.Zones {
			nodeSelectors = append(nodeSelectors, polardbxv1polardbx.NodeSelectorItem{
				Name:         t.newNodeSelectorName(rz.Region, zone),
				NodeSelector: t.newNodeSelector(rz.RegionLabel, rz.Region, rz.ZoneLabel, zone),
			})
		}
	}

	rules := &polardbxv1polardbx.TopologyRules{
		Selectors: nodeSelectors,
	}

	switch mode {
	case RuleGuide1l2c:
		return nil, errors.New("not supported yet")
	case RuleGuide1l3c:
		cnRules := make([]polardbxv1polardbx.StatelessTopologyRuleItem, 0, 3)
		for _, ns := range nodeSelectors {
			cnRules = append(cnRules, polardbxv1polardbx.StatelessTopologyRuleItem{
				Name:     ns.Name,
				Replicas: &intstr.IntOrString{Type: intstr.String, StrVal: "1/3"},
				NodeSelector: &polardbxv1polardbx.NodeSelectorReference{
					Reference: ns.Name,
				},
			})
		}
		rules.Components.CN = cnRules

		dnRules := make([]polardbxv1polardbx.XStoreTopologyRuleNodeSetItem, 0, 3)
		for i, ns := range nodeSelectors {
			role := polardbxv1xstore.RoleCandidate
			namePrefix := "cand"
			if i >= 2 {
				role = polardbxv1xstore.RoleVoter
				namePrefix = "log"
			}
			dnRules = append(dnRules, polardbxv1polardbx.XStoreTopologyRuleNodeSetItem{
				Name:     namePrefix + "-" + ns.Name,
				Role:     role,
				Replicas: 1,
				NodeSelector: &polardbxv1polardbx.NodeSelectorReference{
					Reference: ns.Name,
				},
			})
		}
		rules.Components.DN = &polardbxv1polardbx.XStoreTopologyRule{NodeSets: dnRules}
	case RuleGuide2l3c:
		cnRules := make([]polardbxv1polardbx.StatelessTopologyRuleItem, 0, 3)
		for i, ns := range nodeSelectors {
			replicas := intstr.FromString("2/5")
			if i >= 2 {
				replicas = intstr.FromString("1/5")
			}
			cnRules = append(cnRules, polardbxv1polardbx.StatelessTopologyRuleItem{
				Name:     ns.Name,
				Replicas: &replicas,
				NodeSelector: &polardbxv1polardbx.NodeSelectorReference{
					Reference: ns.Name,
				},
			})
		}
		rules.Components.CN = cnRules

		dnRules := make([]polardbxv1polardbx.XStoreTopologyRuleNodeSetItem, 0, 3)
		for i, ns := range nodeSelectors {
			replicas := 2
			role := polardbxv1xstore.RoleCandidate
			namePrefix := "cand"
			if i >= 2 {
				role = polardbxv1xstore.RoleVoter
				namePrefix = "log"
				replicas = 1
			}
			dnRules = append(dnRules, polardbxv1polardbx.XStoreTopologyRuleNodeSetItem{
				Name:     namePrefix + "-" + ns.Name,
				Role:     role,
				Replicas: int32(replicas),
				NodeSelector: &polardbxv1polardbx.NodeSelectorReference{
					Reference: ns.Name,
				},
			})
		}
		rules.Components.DN = &polardbxv1polardbx.XStoreTopologyRule{NodeSets: dnRules}
	case RuleGuide3l5c:
		return nil, errors.New("not supported yet")
	default:
		return nil, errors.New("invalid mode")
	}

	return rules, nil
}

func NewTopologyRuleFactory(rc *polardbxv1reconcile.Context) TopologyRuleFactory {
	return &topologyRuleFactory{rc: rc}
}
