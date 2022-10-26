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
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1common "github.com/alibaba/polardbx-operator/api/v1/common"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
)

func init() {
	registerHandler(polardbxmeta.AnnotationTopologyModeGuide, &topologyModeHandler{})
}

type topologyModeHandler struct{}

type modeName string

type nodeType string

type nodeConfig struct {
	// replica indicates the number of a specific node, like 3 CN, 2 DN
	replica int32
	// paxosReplica indicates the number of replicas in paxos protocol only for GMS and DN
	// it only has three possible values: 1/3/5, 3 by default
	paxosReplica int32
	// resource indicates resources available for a node
	resource *corev1.ResourceRequirements
}

type topologyMode struct {
	name     modeName
	config   map[nodeType]*nodeConfig
	rules    *polardbxv1polardbx.TopologyRules
	shareGMS bool
}

const (
	// Performance name creates 3 CN, 3 DN, 1 GMS and maximizes the use of resources
	// usage: performance(labels=(zone-a))
	// in which the labels describe where to schedule the nodes
	Performance modeName = "performance"
	// Minimum name creates 1 CN (2c4g) + 1 DN (2c4g), in which GMS in dn-0
	Minimum modeName = "minimum"
	// MinimumCDC name creates 1 CN + 1 DN + 1 GMS + 1 CDC all in 4c8g
	MinimumCDC modeName = "minimum-cdc"
	// Custom name allows user to specific both resources and replicas
	// usage: custom(CN=(1,2c4g),DN=(,4c8g,single-replica),GMS=(1,))
	// in which the former number indicates replicas and the later letters indicate resource
	Custom modeName = "custom"
	// QuickStart creates a 1 GMS + 1 CN + 1 DN + 1 CDC in total resources 4c8g for quick start.
	QuickStart modeName = "quick-start"

	// QuickStartPaxos Change to 3 paxos replicas of DN on the basis of QuickStart
	QuickStartPaxos modeName = "quick-start-paxos"
)

const (
	CN  nodeType = "CN"
	DN  nodeType = "DN"
	GMS nodeType = "GMS"
	CDC nodeType = "CDC"
)

// RollingStrategyLabelName indicates the label name used to build node selectors for performance mode
const RollingStrategyLabelName = "node-configuration"

const MegaByte int64 = 1024 * 1024
const GigaByte int64 = 1024 * 1024 * 1024

var ErrInvalidModeGuide = errors.New("invalid name guide")

func (h *topologyModeHandler) updateTopology(obj *polardbxv1.PolarDBXCluster, mode *topologyMode) error {
	for node, conf := range mode.config {
		switch node {
		case CN:
			conf.resource.DeepCopyInto(&obj.Spec.Topology.Nodes.CN.Template.Resources)
			*obj.Spec.Topology.Nodes.CN.Replicas = conf.replica
		case DN:
			conf.resource.DeepCopyInto(&obj.Spec.Topology.Nodes.DN.Template.Resources.ResourceRequirements)
			obj.Spec.Topology.Nodes.DN.Replicas = conf.replica
		case GMS:
			obj.Spec.Topology.Nodes.GMS = polardbxv1polardbx.TopologyNodeGMS{
				Template: &polardbxv1polardbx.XStoreTemplate{
					Resources: polardbxv1common.ExtendedResourceRequirements{
						ResourceRequirements: *conf.resource,
					},
				},
			}
		case CDC:
			obj.Spec.Topology.Nodes.CDC = &polardbxv1polardbx.TopologyNodeCDC{
				Template: polardbxv1polardbx.CDCTemplate{
					Resources: *conf.resource,
				},
			}
			obj.Spec.Topology.Nodes.CDC.Replicas = conf.replica
		default:
			return errors.New("unexpected type of node")
		}
	}

	if mode.rules != nil {
		mode.rules.DeepCopyInto(&obj.Spec.Topology.Rules)
	}

	// share gms is disabled by default
	obj.Spec.ShareGMS = mode.shareGMS

	return nil
}

func (h *topologyModeHandler) Handle(rc *polardbxv1reconcile.Context, obj *polardbxv1.PolarDBXCluster) error {
	modeGuide := obj.Annotations[polardbxmeta.AnnotationTopologyModeGuide]

	factory := NewTopologyModeFactory(rc)
	mode, err := factory.parseTopologyModeGuide(modeGuide)
	if err != nil {
		return err
	}

	err = h.updateTopology(obj, mode)
	if err != nil {
		return err
	}
	delete(obj.Annotations, polardbxmeta.AnnotationTopologyModeGuide)

	return nil
}

type TopologyModeFactory interface {
	parseTopologyModeGuide(s string) (*topologyMode, error)
}

type topologyModeFactory struct {
	rc *polardbxv1reconcile.Context
}

func NewTopologyModeFactory(rc *polardbxv1reconcile.Context) *topologyModeFactory {
	return &topologyModeFactory{rc: rc}
}

// the bool return value is used to indicate whether user sets a param in performance or minimum mode
func (t *topologyModeFactory) convertibleStringOrDefault(s string, defaultValue int64) (int64, bool) {
	value, err := strconv.Atoi(s)
	if err != nil {
		return defaultValue, false
	}
	return int64(value), true
}

func (t *topologyModeFactory) newResourceRequirements(limitCPU int64, limitMemory int64, requests ...int64) *corev1.ResourceRequirements {
	requirements := &corev1.ResourceRequirements{
		Limits: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    *resource.NewQuantity(limitCPU, resource.DecimalSI),
			corev1.ResourceMemory: *resource.NewQuantity(limitMemory, resource.BinarySI),
		},
	}
	if len(requests) >= 2 {
		requirements.Requests = map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    *resource.NewQuantity(requests[0], resource.DecimalSI),
			corev1.ResourceMemory: *resource.NewQuantity(requests[1], resource.BinarySI),
		}
	}
	return requirements
}

func (t *topologyModeFactory) parseGuideParamsAndUpdateConfig(s string, config map[nodeType]*nodeConfig) {
	pattern := regexp.MustCompile(`(DN|CN|GMS|CDC)=\((\d?)(?:,(?:(\d*)[cC](\d*)[gG])?)?(?:,([^()]+)?)?\),?`)
	matches := pattern.FindAllStringSubmatch(s, -1)

	for _, v := range matches {
		var defaultReplica int64
		if nodeType(v[1]) == DN || nodeType(v[1]) == CN {
			defaultReplica = 2
		} else {
			defaultReplica = 1
		}

		replica, replicaConvertible := t.convertibleStringOrDefault(v[2], defaultReplica)
		cpu, cpuConvertible := t.convertibleStringOrDefault(v[3], 4)
		memory, memoryConvertible := t.convertibleStringOrDefault(v[4], 8)

		// allow user to modify params even in performance or minimum template
		if _, ok := config[nodeType(v[1])]; ok {
			if replicaConvertible {
				config[nodeType(v[1])].replica = int32(replica)
			}

			// cpu & memory must be modified together
			if cpuConvertible && memoryConvertible {
				config[nodeType(v[1])].resource = t.newResourceRequirements(cpu, memory*GigaByte)
			}
		} else {
			// if the node is not included in the template or the mode is custom
			config[nodeType(v[1])] = &nodeConfig{
				replica:  int32(replica),
				resource: t.newResourceRequirements(cpu, memory*GigaByte),
			}
		}

		if nodeType(v[1]) == DN || nodeType(v[1]) == GMS {
			if v[5] == "single-replica" {
				config[nodeType(v[1])].paxosReplica = 1
			} else if v[5] == "five-replica" {
				config[nodeType(v[1])].paxosReplica = 5
			} else {
				config[nodeType(v[1])].paxosReplica = 3
			}
		}
	}
}

// parseLabels only work for performance mode in order to provide rolling strategy
func (t *topologyModeFactory) parseLabels(s string) []string {
	pattern := regexp.MustCompile(`labels?=\(([^()]+)\)`)
	matches := pattern.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return nil
	}

	labels := strings.Split(matches[0][1], "|")
	return labels
}

func (t *topologyModeFactory) newNodeSelector(labels []string) *polardbxv1polardbx.NodeSelectorItem {
	nodeSelectorItem := &polardbxv1polardbx.NodeSelectorItem{
		Name: "performance-mode-selector",
		NodeSelector: corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      RollingStrategyLabelName,
							Operator: corev1.NodeSelectorOpIn,
							Values:   labels,
						},
					},
				},
			},
		},
	}
	return nodeSelectorItem
}

func (t *topologyModeFactory) newXStoreTopologyRule(replica int32, nodeSelectorItems []polardbxv1polardbx.NodeSelectorItem) *polardbxv1polardbx.XStoreTopologyRule {
	var rule *polardbxv1polardbx.XStoreTopologyRule
	if len(nodeSelectorItems) > 0 {
		rule = &polardbxv1polardbx.XStoreTopologyRule{
			Rolling: &polardbxv1polardbx.XStoreTopologyRuleRolling{
				Replicas: replica,
				NodeSelector: &polardbxv1polardbx.NodeSelectorReference{
					Reference:    nodeSelectorItems[0].Name,
					NodeSelector: &nodeSelectorItems[0].NodeSelector,
				},
			},
		}
		return rule
	}

	if replica == 1 {
		rule = &polardbxv1polardbx.XStoreTopologyRule{
			NodeSets: []polardbxv1polardbx.XStoreTopologyRuleNodeSetItem{
				{
					Name:     "single",
					Role:     polardbxv1xstore.RoleCandidate,
					Replicas: 1,
				},
			},
		}
	} else if replica != 0 { // replica can only equal to 1/3/5
		rule = &polardbxv1polardbx.XStoreTopologyRule{
			NodeSets: []polardbxv1polardbx.XStoreTopologyRuleNodeSetItem{
				{
					Name:     "cands",
					Role:     polardbxv1xstore.RoleCandidate,
					Replicas: replica - 1,
				},
				{
					Name:     "log",
					Role:     polardbxv1xstore.RoleVoter,
					Replicas: 1,
				},
			},
		}
	}

	return rule
}

// newStatelessRule generates stateless rule only for performance mode
func (t *topologyModeFactory) newStatelessRule(replica int32, nodeSelectorItems []polardbxv1polardbx.NodeSelectorItem) *polardbxv1polardbx.StatelessTopologyRuleItem {
	if len(nodeSelectorItems) == 0 {
		return nil
	}
	rule := &polardbxv1polardbx.StatelessTopologyRuleItem{
		Name:     "performance",
		Replicas: &intstr.IntOrString{Type: intstr.Int, IntVal: replica},
		NodeSelector: &polardbxv1polardbx.NodeSelectorReference{
			Reference: nodeSelectorItems[0].Name,
		},
	}
	return rule
}

func (t *topologyModeFactory) computeResourcesForPerformanceMode(nodeList *corev1.NodeList) (map[nodeType]*nodeConfig, error) {
	nodes := nodeList.Items
	if len(nodes) < 3 {
		return nil, errors.New("nodes insufficient for performance mode")
	}

	// sort nodes in the decrease order of cpu (suppose that memory has the same order
	// performance will use the three nodes which have the most resources
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Status.Capacity.Cpu().Cmp(*nodes[j].Status.Capacity.Cpu()) == 1
	})
	if !reflect.DeepEqual(nodes[0].Status.Capacity, nodes[2].Status.Capacity) {
		return nil, errors.New("performance mode is not supported for nodes with different resources")
	}

	totalCPU, _ := nodes[0].Status.Capacity.Cpu().AsInt64()
	totalMemory, _ := nodes[0].Status.Capacity.Memory().AsInt64()

	// leave at least 2 cpu and 4g for logger, in addition to a gap of 1 cpu and 512m
	totalCPU -= 3
	totalMemory -= 9 * 512 * MegaByte

	// 2 DN + 1 CN in one node, which need 3.5 pieces of resources that DN request
	// For the convenience of calculation, here set pieces to 4
	var pieces int64 = 4
	unitCPU := totalCPU / pieces
	unitMemory := totalMemory / pieces

	config := map[nodeType]*nodeConfig{
		CN: {
			replica:  3,
			resource: t.newResourceRequirements(unitCPU/2+unitCPU, unitMemory/2+unitMemory),
		},
		DN: {
			replica:      3,
			paxosReplica: 3,
			resource:     t.newResourceRequirements(unitCPU, unitMemory),
		},
		GMS: {
			replica:      1,
			paxosReplica: 3,
			resource:     t.newResourceRequirements(4, 8*GigaByte, 1, GigaByte),
		},
	}

	return config, nil
}

func (t *topologyModeFactory) computeResourcesForMinimumMode(withCDC bool) map[nodeType]*nodeConfig {
	var config map[nodeType]*nodeConfig
	if withCDC {
		config = map[nodeType]*nodeConfig{
			CN: {
				replica:  1,
				resource: t.newResourceRequirements(4, 8*GigaByte),
			},
			DN: {
				replica:  1,
				resource: t.newResourceRequirements(4, 8*GigaByte),
			},
			GMS: {
				replica:  1,
				resource: t.newResourceRequirements(4, 8*GigaByte),
			},
			CDC: {
				replica:  1,
				resource: t.newResourceRequirements(4, 8*GigaByte),
			},
		}
	} else {
		config = map[nodeType]*nodeConfig{
			CN: {
				replica:  1,
				resource: t.newResourceRequirements(2, 4*GigaByte),
			},
			DN: {
				replica:  1,
				resource: t.newResourceRequirements(2, 4*GigaByte),
			},
		}
	}

	return config
}

func (t *topologyModeFactory) newQuickStartModeConfig() map[nodeType]*nodeConfig {
	storeResources := corev1.ResourceRequirements{
		Limits: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Requests: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("500Mi"),
		},
	}

	return map[nodeType]*nodeConfig{
		GMS: {
			replica:      1,
			paxosReplica: 1,
			resource:     &storeResources,
		},
		CN: {
			replica: 1,
			resource: &corev1.ResourceRequirements{
				Limits: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("2Gi"),
				},
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
		},
		DN: {
			replica:      1,
			paxosReplica: 1,
			resource:     &storeResources,
		},
		CDC: {
			replica: 1,
			resource: &corev1.ResourceRequirements{
				Limits: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("500Mi"),
				},
			},
		},
	}
}

func (t *topologyModeFactory) newQuickStartPaxosModeConfig() map[nodeType]*nodeConfig {
	storeResources := corev1.ResourceRequirements{
		Limits: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Requests: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("500Mi"),
		},
	}

	return map[nodeType]*nodeConfig{
		GMS: {
			replica:      1,
			paxosReplica: 3,
			resource:     &storeResources,
		},
		CN: {
			replica: 1,
			resource: &corev1.ResourceRequirements{
				Limits: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("2Gi"),
				},
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
		},
		DN: {
			replica:      1,
			paxosReplica: 3,
			resource:     &storeResources,
		},
		CDC: {
			replica: 1,
			resource: &corev1.ResourceRequirements{
				Limits: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("500Mi"),
				},
			},
		},
	}
}

func (t *topologyModeFactory) parseTopologyModeGuide(s string) (*topologyMode, error) {
	s = strings.TrimSpace(s)
	mode := &topologyMode{}

	// labels only used by performance mode so far
	var labels []string

	// a template will be first generated for performance or minimum mode
	switch {
	case strings.HasPrefix(s, string(Performance)):
		mode.name = Performance
		nodeList := &corev1.NodeList{}
		err := t.rc.Client().List(t.rc.Context(), nodeList)
		if err != nil {
			return nil, err
		}
		config, err := t.computeResourcesForPerformanceMode(nodeList)
		if err != nil {
			return nil, err
		}
		mode.config = config
		labels = t.parseLabels(s)
	case strings.HasPrefix(s, string(MinimumCDC)):
		mode.name = MinimumCDC
		mode.config = t.computeResourcesForMinimumMode(true)
	case strings.HasPrefix(s, string(Minimum)):
		mode.name = Minimum
		mode.config = t.computeResourcesForMinimumMode(false)
		mode.shareGMS = true
	case strings.HasPrefix(s, string(QuickStartPaxos)):
		mode.name = QuickStartPaxos
		mode.config = t.newQuickStartPaxosModeConfig()
		mode.shareGMS = false
	case strings.HasPrefix(s, string(QuickStart)):
		mode.name = QuickStart
		mode.config = t.newQuickStartModeConfig()
		mode.shareGMS = false
	case strings.HasPrefix(s, string(Custom)):
		mode.name = Custom
	default:
		return nil, ErrInvalidModeGuide
	}

	// if any params given in performance or minimum mode, the existing template will be overwritten
	if mode.config == nil {
		mode.config = make(map[nodeType]*nodeConfig)
	}
	t.parseGuideParamsAndUpdateConfig(s, mode.config)

	mode.rules = &polardbxv1polardbx.TopologyRules{}
	if labels != nil {
		nodeSelectorItem := t.newNodeSelector(labels)
		mode.rules.Selectors = []polardbxv1polardbx.NodeSelectorItem{*nodeSelectorItem}
	}

	for node, conf := range mode.config {
		switch node {
		case CN:
			statelessRule := t.newStatelessRule(conf.replica, mode.rules.Selectors)
			if statelessRule != nil {
				mode.rules.Components.CN = []polardbxv1polardbx.StatelessTopologyRuleItem{
					*statelessRule,
				}
			}
		case DN:
			mode.rules.Components.DN = t.newXStoreTopologyRule(conf.paxosReplica, mode.rules.Selectors)
		case GMS:
			mode.rules.Components.GMS = t.newXStoreTopologyRule(conf.paxosReplica, mode.rules.Selectors)
		}
	}

	return mode, nil
}
