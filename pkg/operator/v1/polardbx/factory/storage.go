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

package factory

import (
	"errors"
	"fmt"
	"gopkg.in/ini.v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"strconv"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1common "github.com/alibaba/polardbx-operator/api/v1/common"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	copyutil "github.com/alibaba/polardbx-operator/pkg/util/copy"
	"github.com/alibaba/polardbx-operator/pkg/util/defaults"
	iniutil "github.com/alibaba/polardbx-operator/pkg/util/ini"
)

func (f *objectFactory) getTopologyNodeRuleForGMS(polardbx *polardbxv1.PolarDBXCluster) *polardbxv1polardbx.XStoreTopologyRule {
	topology := polardbx.Status.SpecSnapshot.Topology

	if polardbx.Spec.ShareGMS {
		return topology.Rules.Components.DN
	} else {
		if topology.Rules.Components.GMS != nil {
			return topology.Rules.Components.GMS
		}
		return topology.Rules.Components.DN
	}
}

func (f *objectFactory) getTopologyNodeTemplateForGMS(polardbx *polardbxv1.PolarDBXCluster) *polardbxv1polardbx.XStoreTemplate {
	topology := polardbx.Status.SpecSnapshot.Topology

	var template *polardbxv1polardbx.XStoreTemplate = nil
	if polardbx.Spec.ShareGMS {
		template = &topology.Nodes.DN.Template
	} else if topology.Nodes.GMS.Template != nil {
		template = topology.Nodes.GMS.Template
	} else {
		template = &topology.Nodes.DN.Template
	}

	return template
}
func (f *objectFactory) newPodAnnotations(polardbx *polardbxv1.PolarDBXCluster) map[string]string {
	config := f.rc.Config()

	annotations := map[string]string{}
	if config.Cluster().EnableAliyunAckResourceController() {
		annotations["cpuset-scheduler"] = "true"
	}
	if config.Cluster().EnableDebugModeForComputeNodes() {
		annotations["runmode"] = "debug"
	}

	return annotations
}

var (
	loggerCpuLimit    = resource.MustParse("2")
	loggerMemoryLimit = resource.MustParse("4Gi")
)

func (f *objectFactory) newXStoreNodeResources(template *polardbxv1polardbx.XStoreTemplate,
	role polardbxv1xstore.NodeRole) *polardbxv1common.ExtendedResourceRequirements {
	res := *template.Resources.DeepCopy()

	// Give logger at most 2C8G
	if role == polardbxv1xstore.RoleVoter || role == polardbxv1xstore.RoleLogger {
		cpuLimit, memoryLimit := res.Limits.Cpu(), res.Limits.Memory()
		cpuRequest, memoryRequest := res.Requests.Cpu(), res.Requests.Memory()

		if cpuLimit.Value() > loggerCpuLimit.Value() {
			res.Limits[corev1.ResourceCPU] = loggerCpuLimit
		}
		if memoryLimit.Value() > loggerMemoryLimit.Value() {
			res.Limits[corev1.ResourceMemory] = loggerMemoryLimit
		}
		if cpuRequest.Value() > loggerCpuLimit.Value() {
			res.Requests[corev1.ResourceCPU] = loggerCpuLimit
		}
		if memoryRequest.Value() > loggerMemoryLimit.Value() {
			res.Requests[corev1.ResourceMemory] = loggerMemoryLimit
		}
	}

	return &res
}

func (f *objectFactory) getNodeSelectorFromRef(polardbx *polardbxv1.PolarDBXCluster,
	ref *polardbxv1polardbx.NodeSelectorReference) (*corev1.NodeSelector, error) {
	if ref == nil {
		return nil, nil
	}
	if ref.NodeSelector != nil {
		return ref.NodeSelector.DeepCopy(), nil
	}
	if len(ref.Reference) == 0 {
		return nil, nil
	}
	topology := polardbx.Status.SpecSnapshot.Topology
	for _, item := range topology.Rules.Selectors {
		if item.Name == ref.Reference {
			return item.NodeSelector.DeepCopy(), nil
		}
	}
	return nil, errors.New("undefined node selector: " + ref.Reference)
}

func (f *objectFactory) newXStoreNodeSetAffinity(polardbx *polardbxv1.PolarDBXCluster,
	nodeSelector *corev1.NodeSelector) *corev1.Affinity {
	// Try scatter between DN & GMS
	affinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: nodeSelector,
		},
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								polardbxmeta.LabelName: polardbx.Name,
							},
						},
						Namespaces:  []string{polardbx.Namespace},
						TopologyKey: corev1.LabelHostname,
					},
				},
			},
		},
	}

	return affinity
}

func xstoreNodeTemplateWithResources(template *polardbxv1xstore.NodeTemplate,
	resources *polardbxv1common.ExtendedResourceRequirements) *polardbxv1xstore.NodeTemplate {
	t := template.DeepCopy()
	t.Spec.Resources = resources
	return t
}

func xstoreNodeTemplateWithAffinityAndResources(template *polardbxv1xstore.NodeTemplate,
	affinity *corev1.Affinity, resources *polardbxv1common.ExtendedResourceRequirements) *polardbxv1xstore.NodeTemplate {
	t := template.DeepCopy()
	t.Spec.Affinity = affinity
	t.Spec.Resources = resources
	return t
}

func (f *objectFactory) newXStoreNodeSets(polardbx *polardbxv1.PolarDBXCluster,
	template *polardbxv1polardbx.XStoreTemplate,
	nodeTemplate *polardbxv1xstore.NodeTemplate,
	rule *polardbxv1polardbx.XStoreTopologyRule,
	rollingNodeIdx int) ([]polardbxv1xstore.NodeSet, error) {
	if rule == nil || (rule.Rolling == nil && rule.NodeSets == nil) {
		if !featuregate.EnableGalaxyClusterMode.Enabled() {
			// Set default node to be single nodes if engine is galaxy
			// and cluster mode isn't enabled.
			if template.Engine == "galaxy" {
				if !polardbx.Spec.Readonly {
					return []polardbxv1xstore.NodeSet{
						{
							Role:     polardbxv1xstore.RoleCandidate,
							Replicas: 1,
							Template: xstoreNodeTemplateWithResources(
								nodeTemplate,
								f.newXStoreNodeResources(template, polardbxv1xstore.RoleCandidate),
							),
						},
					}, nil
				} else {
					return []polardbxv1xstore.NodeSet{
						{
							Role:     polardbxv1xstore.RoleLearner,
							Replicas: 1,
							Template: xstoreNodeTemplateWithResources(
								nodeTemplate,
								f.newXStoreNodeResources(template, polardbxv1xstore.RoleLearner),
							),
						},
					}, nil
				}
			}
		}

		if !polardbx.Spec.Readonly {
			return []polardbxv1xstore.NodeSet{
				{
					Name:     "cand",
					Role:     polardbxv1xstore.RoleCandidate,
					Replicas: 2,
					Template: xstoreNodeTemplateWithResources(
						nodeTemplate,
						f.newXStoreNodeResources(template, polardbxv1xstore.RoleCandidate),
					),
				},
				{
					Name:     "log",
					Role:     polardbxv1xstore.RoleVoter,
					Replicas: 1,
					Template: xstoreNodeTemplateWithResources(
						nodeTemplate,
						f.newXStoreNodeResources(template, polardbxv1xstore.RoleVoter),
					),
				},
			}, nil
		} else {
			return []polardbxv1xstore.NodeSet{
				{
					Name:     "learner",
					Role:     polardbxv1xstore.RoleLearner,
					Replicas: 1,
					Template: xstoreNodeTemplateWithResources(
						nodeTemplate,
						f.newXStoreNodeResources(template, polardbxv1xstore.RoleLearner),
					),
				},
			}, nil
		}
	} else if rule.Rolling != nil {
		rs := rule.Rolling
		if rs.Replicas&1 == 0 {
			return nil, errors.New("invalid rolling strategy, even replicas")
		}

		nodeSelector, err := f.getNodeSelectorFromRef(polardbx, rule.Rolling.NodeSelector)
		if err != nil {
			return nil, err
		}
		nodes, err := f.rc.GetSortedSchedulableNodes(nodeSelector)
		if err != nil {
			return nil, err
		}
		if len(nodes) == 0 {
			return nil, errors.New("no nodes found can be scheduled to")
		}

		nodeSets := make([]polardbxv1xstore.NodeSet, 0, int(rs.Replicas))
		for i := 0; i < int(rs.Replicas); i++ {
			name, role := fmt.Sprintf("cand-%d", i), polardbxv1xstore.RoleCandidate
			if polardbx.Spec.Readonly {
				name, role = fmt.Sprintf("lear-%d", i), polardbxv1xstore.RoleLearner
			} else if i != 0 && i == int(rs.Replicas-1) {
				name, role = fmt.Sprintf("log-%d", i), polardbxv1xstore.RoleVoter
			}

			affinity := f.newXStoreNodeSetAffinity(polardbx, &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      corev1.LabelHostname,
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{nodes[(i+rollingNodeIdx)%len(nodes)].Name},
							},
						},
					},
				},
			})
			nodeSets = append(nodeSets, polardbxv1xstore.NodeSet{
				Name:     name,
				Role:     role,
				Replicas: 1,
				Template: xstoreNodeTemplateWithAffinityAndResources(nodeTemplate,
					affinity, f.newXStoreNodeResources(template, role)),
			})
		}
		return nodeSets, nil
	} else {
		// Validate the rule
		nodeSetNames := make(map[string]struct{})
		candidatesCnt, votersCnt, learnerCnt := 0, 0, 0
		for _, ns := range rule.NodeSets {
			if ns.Role == polardbxv1xstore.RoleCandidate {
				candidatesCnt += int(ns.Replicas)
			} else if ns.Role == polardbxv1xstore.RoleVoter {
				votersCnt += int(ns.Replicas)
			} else if ns.Role == polardbxv1xstore.RoleLearner {
				learnerCnt += int(ns.Replicas)
			}
			if ns.Replicas == 0 {
				return nil, errors.New("invalid xstore topology rule: replicas is zero")
			}
			if _, ok := nodeSetNames[ns.Name]; ok {
				return nil, errors.New("invalid xstore topology rule: duplicate node set name " + ns.Name)
			}
			nodeSetNames[ns.Name] = struct{}{}
		}
		if polardbx.Spec.Readonly {
			if learnerCnt == 0 {
				return nil, errors.New("invalid readonly xstore topology rule: no learner found")
			}
			if candidatesCnt != 0 || votersCnt != 0 {
				return nil, errors.New("invalid readonly xstore topoly rule: containing votes")
			}
		} else {
			if candidatesCnt == 0 {
				return nil, errors.New("invalid xstore topology rule: no candidate found")
			}
			if (candidatesCnt+votersCnt)&1 == 0 {
				return nil, errors.New("invalid xstore topology rule: even voters")
			}
		}

		// Build node sets.
		nodeSets := make([]polardbxv1xstore.NodeSet, 0, len(rule.NodeSets))
		for _, nsRule := range rule.NodeSets {
			var affinity *corev1.Affinity = nil
			nodeSelector, err := f.getNodeSelectorFromRef(polardbx, nsRule.NodeSelector)
			if err != nil {
				return nil, err
			}
			if nodeSelector != nil {
				affinity = f.newXStoreNodeSetAffinity(polardbx, nodeSelector)
			}
			nodeSets = append(nodeSets, polardbxv1xstore.NodeSet{
				Name:     nsRule.Name,
				Role:     nsRule.Role,
				Replicas: nsRule.Replicas,
				Template: xstoreNodeTemplateWithAffinityAndResources(nodeTemplate,
					affinity, f.newXStoreNodeResources(template, nsRule.Role)),
			})
		}

		return nodeSets, nil
	}
}

func (f *objectFactory) newXStore(
	polardbx *polardbxv1.PolarDBXCluster,
	name string,
	restoreName string,
	rule *polardbxv1polardbx.XStoreTopologyRule,
	template *polardbxv1polardbx.XStoreTemplate,
	mycnfOverlay string,
	labels map[string]string,
	annotations map[string]string,
	rollingNodeIndex int,
) (*polardbxv1.XStore, error) {
	topology := polardbx.Status.SpecSnapshot.Topology
	// Determine log purge interval
	dnConfig := polardbx.Spec.Config.DN
	logPurgeInterval := dnConfig.LogPurgeInterval
	logDataSeparation := dnConfig.LogDataSeparation
	staticConfig := polardbx.Spec.Config.CN.Static
	rpcProtocolVersion := intstr.FromInt(1)
	if staticConfig != nil {
		rpcProtocolVersion = staticConfig.RPCProtocolVersion
	}

	// Determine version.
	engine := template.Engine

	// Determine parameter template.
	templateName := polardbx.Spec.ParameterTemplate.Name
	templateNameSpace := polardbx.Spec.ParameterTemplate.Namespace

	// Build
	affinity := f.newXStoreNodeSetAffinity(polardbx, nil)
	nodeTemplate := &polardbxv1xstore.NodeTemplate{
		ObjectMeta: polardbxv1common.PartialObjectMeta{
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: polardbxv1xstore.NodeSpec{
			Image: defaults.NonEmptyStrOrDefault(
				template.Image,
				f.rc.Config().Images().DefaultImageForStore(engine, xstoreconvention.ContainerEngine, topology.Version),
			),
			ImagePullPolicy:  template.ImagePullPolicy,
			ImagePullSecrets: template.ImagePullSecrets,
			HostNetwork:      template.HostNetwork,
			Affinity:         affinity,
		},
	}

	nodeSets, err := f.newXStoreNodeSets(polardbx, template, nodeTemplate, rule, rollingNodeIndex)
	if err != nil {
		return nil, err
	}

	primaryXStoreName := name

	if polardbx.Spec.Readonly {
		primaryPolardbx, err := f.rc.GetPrimaryPolarDBX()
		if err != nil {
			return nil, err
		}
		primaryXStoreName = convention.NewDNName(primaryPolardbx, rollingNodeIndex)
	}

	xstore := &polardbxv1.XStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: polardbx.Namespace,
			Labels: k8shelper.PatchLabels(
				copyutil.CopyStrMap(labels),
				map[string]string{
					polardbxmeta.LabelGeneration: strconv.FormatInt(polardbx.Status.ObservedGeneration, 10),
					xstoremeta.LabelPrimaryName:  primaryXStoreName,
				},
			),
			Annotations: map[string]string{
				// Set to empty rand to avoid long xstore/pod names.
				xstoremeta.AnnotationGuideRand:                   "",
				polardbxv1common.AnnotationOperatorCreateVersion: polardbx.Annotations[polardbxv1common.AnnotationOperatorCreateVersion],
			},
			Finalizers: []string{polardbxmeta.Finalizer},
		},
		Spec: polardbxv1.XStoreSpec{
			Engine:         template.Engine,
			ServiceLabels:  labels,
			ServiceType:    template.ServiceType,
			Readonly:       polardbx.Spec.Readonly,
			PrimaryCluster: polardbx.Spec.PrimaryCluster,
			PrimaryXStore:  primaryXStoreName,
			Config: polardbxv1xstore.Config{
				Dynamic: polardbxv1xstore.ControllerConfig{
					LogPurgeInterval:   &logPurgeInterval,
					DiskQuota:          template.DiskQuota,
					LogDataSeparation:  logDataSeparation,
					RpcProtocolVersion: rpcProtocolVersion,
				},
				Engine: polardbxv1xstore.EngineConfig{
					Override: &polardbxv1common.Value{
						Value: &mycnfOverlay,
					},
				},
				Envs: polardbx.Spec.Config.DN.Envs,
			},
			Topology: polardbxv1xstore.Topology{
				NodeSets: nodeSets,
			},
			ParameterTemplate: polardbxv1xstore.ParameterTemplate{
				Namespace: templateNameSpace,
				Name:      templateName,
			},
			TDE: polardbxv1.TDE{
				Enable:      polardbx.Spec.TDE.Enable,
				KeyringPath: polardbx.Spec.TDE.KeyringPath,
			},
			Exclusive: polardbx.Spec.Exclusive,
		},
	}
	restoreOpt := polardbx.Spec.Restore
	if polardbx.Status.Phase == polardbxv1polardbx.PhaseRestoring && restoreOpt != nil {
		if restoreOpt.BackupSet == "" || len(restoreOpt.BackupSet) == 0 {
			xstore.Spec.Restore = &polardbxv1.XStoreRestoreSpec{
				From: polardbxv1.XStoreRestoreFrom{
					XStoreName: restoreName,
				},
				Time:     restoreOpt.Time,
				TimeZone: restoreOpt.TimeZone,
			}
		} else {
			backupSet, err := f.GetXStoreBackupName(restoreOpt.BackupSet, restoreName)
			if err != nil {
				return nil, err
			}
			var pitrEndpoint string
			if polardbx.Status.PitrStatus != nil {
				pitrEndpoint = polardbx.Status.PitrStatus.PrepareJobEndpoint
			}
			xstore.Spec.Restore = &polardbxv1.XStoreRestoreSpec{
				BackupSet: backupSet,
				From: polardbxv1.XStoreRestoreFrom{
					XStoreName: restoreName,
				},
				PitrEndpoint: pitrEndpoint,
			}

		}
	}

	return xstore, nil
}

func (f *objectFactory) GetOriginalXstoreNameForRestore(polardbx polardbxv1.PolarDBXCluster, name string) (string, error) {
	backup := &polardbxv1.PolarDBXBackup{}
	var err error
	if polardbx.Spec.Restore.BackupSet == "" && len(polardbx.Spec.Restore.BackupSet) == 0 {
		backup, err = f.rc.GetCompletedPXCBackup(map[string]string{polardbxmeta.LabelName: polardbx.Spec.Restore.From.PolarBDXName})
	} else {
		backup, err = f.rc.GetPXCBackupByName(polardbx.Spec.Restore.BackupSet)
	}
	if err != nil {
		return "", err
	}
	for _, xstoreName := range backup.Status.XStores {
		if xstoreName[len(xstoreName)-4:] == name[len(name)-4:] { // safe when quantity of dn less than 10000
			return xstoreName, nil
		}
	}
	return "", errors.New("failed to find matched xstore")
}

func (f *objectFactory) GetXStoreBackupName(backupName, xstoreName string) (string, error) {
	backup, err := f.rc.GetPXCBackupByName(backupName)
	if err != nil {
		return "", err
	}
	if backup != nil {
		xstoreBackupName, ok := backup.Status.Backups[xstoreName]
		if ok {
			return xstoreBackupName, nil
		}
	}
	return "", errors.New("failed to get xstore backup name")
}

func (f *objectFactory) newMycnfOverlayInfFile(polardbxstore *polardbxv1.PolarDBXCluster, enforceTso bool) (*ini.File, error) {
	config := polardbxstore.Spec.Config.DN

	mycnfValue := config.MycnfOverwrite

	file, err := ini.LoadSources(ini.LoadOptions{
		AllowBooleanKeys:           true,
		AllowPythonMultilineValues: true,
		SpaceBeforeInlineComment:   true,
		PreserveSurroundedQuote:    true,
		IgnoreInlineComment:        true,
	}, []byte(mycnfValue))
	if err != nil {
		return nil, err
	}

	if enforceTso {
		file.Section("").Key("loose_enable_gts").SetValue("1")
	}

	staticConfig := polardbxstore.Spec.Config.CN.Static
	var useNewRpc string
	if staticConfig != nil {
		if staticConfig.RPCProtocolVersion.String() == "1" {
			useNewRpc = "off"
		} else {
			useNewRpc = "on"
		}
	}
	file.Section("").Key("loose_new_rpc").SetValue(useNewRpc)

	return file, nil
}

func (f *objectFactory) newMycnfOverlay(polardbxstore *polardbxv1.PolarDBXCluster, enforceTso bool) (string, error) {
	file, err := f.newMycnfOverlayInfFile(polardbxstore, enforceTso)
	if err != nil {
		return "", err
	}
	return iniutil.ToString(file), nil
}

func (f *objectFactory) NewXStoreMyCnfOverlay4GMS() (string, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return "", err
	}
	return f.newMycnfOverlay(polardbx, true)
}

func (f *objectFactory) NewXStoreGMS() (*polardbxv1.XStore, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}
	template := f.getTopologyNodeTemplateForGMS(polardbx)
	rule := f.getTopologyNodeRuleForGMS(polardbx)

	mycnfOverlay, err := f.NewXStoreMyCnfOverlay4GMS()
	if err != nil {
		return nil, err
	}
	xstoreName := convention.NewGMSName(polardbx)

	restoreName := ""
	if polardbx.Status.Phase == polardbxv1polardbx.PhaseRestoring {
		restoreName, err = f.GetOriginalXstoreNameForRestore(*polardbx, xstoreName)
		if err != nil {
			return nil, err
		}
	}

	return f.newXStore(
		polardbx,
		xstoreName,
		restoreName,
		rule,
		template,
		mycnfOverlay,
		convention.ConstLabelsWithRole(polardbx, polardbxmeta.RoleGMS),
		f.newPodAnnotations(polardbx),
		0,
	)
}

func (f *objectFactory) NewXStoreMyCnfOverlay4DN(idx int) (string, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return "", err
	}

	return f.newMycnfOverlay(polardbx, polardbx.Spec.ShareGMS && idx == 0)
}

func (f *objectFactory) NewXStoreDN(idx int) (*polardbxv1.XStore, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}

	topology := polardbx.Status.SpecSnapshot.Topology
	template := &topology.Nodes.DN.Template
	mycnfOverlay, err := f.NewXStoreMyCnfOverlay4DN(idx)
	if err != nil {
		return nil, err
	}

	xstoreName := convention.NewDNName(polardbx, idx)
	restoreName := ""
	if polardbx.Status.Phase == polardbxv1polardbx.PhaseRestoring {
		restoreName, err = f.GetOriginalXstoreNameForRestore(*polardbx, xstoreName)
		if err != nil {
			return nil, err
		}
	}
	xstore, err := f.newXStore(
		polardbx,
		xstoreName,
		restoreName,
		topology.Rules.Components.DN,
		template,
		mycnfOverlay,
		convention.ConstLabelsForDN(polardbx, idx),
		f.newPodAnnotations(polardbx),
		idx,
	)

	if err == nil && xstore != nil {
		convention.AddLabelHash(xstoremeta.LabelHash, xstore)
	}

	return xstore, err
}
