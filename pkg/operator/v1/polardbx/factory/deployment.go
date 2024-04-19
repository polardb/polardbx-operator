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
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/k8s/helper/selector"
	dictutil "github.com/alibaba/polardbx-operator/pkg/util/dict"
	maputil "github.com/alibaba/polardbx-operator/pkg/util/map"
	"github.com/alibaba/polardbx-operator/pkg/util/math"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	"github.com/alibaba/polardbx-operator/pkg/probe"
	copyutil "github.com/alibaba/polardbx-operator/pkg/util/copy"
	"github.com/alibaba/polardbx-operator/pkg/util/defaults"
)

type matchingRule struct {
	replicas int
	rule     *polardbxv1polardbx.StatelessTopologyRuleItem
}

func (f *objectFactory) getRuleReplicas(total int, rule *polardbxv1polardbx.StatelessTopologyRuleItem) (int, error) {
	if rule.Replicas.Type == intstr.Int {
		val := rule.Replicas.IntValue()
		return val, nil
	} else {
		s := rule.Replicas.StrVal
		if strings.HasSuffix(s, "%") || strings.HasSuffix(s, "%+") {
			var percentageStr string
			roundUp := false
			if s[len(s)-1] == '+' {
				percentageStr = s[:len(s)-2]
				roundUp = true
			} else {
				percentageStr = s[:len(s)-1]
			}
			percentage, err := strconv.Atoi(strings.TrimSpace(percentageStr))
			if err != nil {
				return 0, fmt.Errorf("invalid replicas: not a percentage, %w", err)
			}
			if percentage >= 100 {
				return 0, fmt.Errorf("invalid replicas: not a valid percentage, should be less than 1")
			}

			if roundUp {
				return (total*percentage + 99) / 100, nil
			} else {
				return total * percentage / 100, nil
			}
		} else if strings.Contains(s, "/") {
			split := strings.SplitN(s, "/", 2)
			if len(split) < 2 {
				return 0, fmt.Errorf("invalid replicas: not a fraction")
			}
			a, err := strconv.Atoi(strings.TrimSpace(split[0]))
			if err != nil {
				return 0, fmt.Errorf("invalid replicas: not a fraction, %w", err)
			}
			b, err := strconv.Atoi(strings.TrimSpace(split[1]))
			if err != nil {
				return 0, fmt.Errorf("invalid replicas: not a fraction, %w", err)
			}
			if a < 0 {
				return 0, fmt.Errorf("invalid replicas: not a valid fraction, numerator should be non-negative integer")
			}
			if b <= 0 {
				return 0, fmt.Errorf("invalid replicas: not a valid fraction, denominator should be positive integer")
			}
			if a >= b {
				return 0, fmt.Errorf("invalid replicas: not a valid fraction, should be less than 1")
			}
			return total * a / b, nil
		} else {
			val, err := strconv.Atoi(rule.Replicas.StrVal)
			if err != nil {
				return 0, fmt.Errorf("invalid replicas: %w", err)
			}
			return val, nil
		}
	}
}

func sortedMatchingRuleNames(matchingRules map[string]matchingRule) []string {
	keys := make([]string, 0, len(matchingRules))
	for key := range matchingRules {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (f *objectFactory) getStatelessMatchingRules(replicas int, hostNetwork bool, rules []polardbxv1polardbx.StatelessTopologyRuleItem) (map[string]matchingRule, error) {
	// Build rule replicas
	ruleReplicas := make(map[string]matchingRule)
	var defaultRule *polardbxv1polardbx.StatelessTopologyRuleItem = nil
	ruleDeclaredReplicas := 0
	for i := range rules {
		rule := rules[i]
		if rule.Name == "" {
			return nil, fmt.Errorf("invalid rule: empty name")
		}
		_, exist := ruleReplicas[rule.Name]
		if exist {
			return nil, fmt.Errorf("invalid rules: duplicate rule names %s", rule.Name)
		}
		if rule.Replicas == nil {
			if defaultRule != nil {
				return nil, fmt.Errorf("invalid rules: multiple default rule found %s", rule.Name)
			}
			defaultRule = &rule
			continue
		}
		r, err := f.getRuleReplicas(replicas, &rule)
		if err != nil {
			return nil, err
		}
		if r == 0 {
			continue
		}
		ruleDeclaredReplicas += r
		if ruleDeclaredReplicas > replicas {
			return nil, fmt.Errorf("invalid rules: declared replicas is larger than total")
		}
		ruleReplicas[rule.Name] = matchingRule{
			replicas: r,
			rule:     rule.DeepCopy(),
		}
	}

	// Set up default rule.
	defaultRuleName := ""
	if defaultRule != nil {
		defaultRuleName = defaultRule.Name
	}
	if f.buildContext.CdcGroup != nil && defaultRuleName == "" {
		defaultRuleName = f.buildContext.CdcGroup.Name
	}
	if replicas > ruleDeclaredReplicas {
		ruleReplicas[defaultRuleName] = matchingRule{
			replicas: replicas - ruleDeclaredReplicas,
			rule:     defaultRule,
		}
	}

	// Build matching rules.
	matchingRules := ruleReplicas

	// If pods run in host network mode, we have to distinguish every pod's ports. Thus,
	// sets are in the form of one-deployment-per-pod.
	// Otherwise, we just calculate the replicas according to the rule.
	if hostNetwork {
		sortedRuleNames := sortedMatchingRuleNames(matchingRules)
		currentMatchingRuleIdx, usedReplicas := 0, 0
		hostNetworkRules := make(map[string]matchingRule)
		mr := matchingRules[sortedRuleNames[currentMatchingRuleIdx]]
		for i := 0; i < replicas; i++ {
			hostNetworkRules[fmt.Sprintf("host-%d", i)] = matchingRule{
				rule:     mr.rule,
				replicas: 1,
			}
			usedReplicas++

			// update matching rule if it's not the last
			if i < replicas-1 && usedReplicas >= mr.replicas {
				usedReplicas = 0
				currentMatchingRuleIdx++
				mr = matchingRules[sortedRuleNames[currentMatchingRuleIdx]]
			}
		}
		return hostNetworkRules, nil
	} else {
		return matchingRules, nil
	}
}

func (f *objectFactory) tryScatterAffinityForStatelessDeployment(labels map[string]string, nodeSelector *corev1.NodeSelector) *corev1.Affinity {
	affinity := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: labels,
						},
						TopologyKey: corev1.LabelHostname,
					},
				},
			},
		},
	}
	if nodeSelector != nil {
		affinity.NodeAffinity = &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: nodeSelector,
		}
	}
	return affinity
}

func (f *objectFactory) getGmsConn(polardb *polardbxv1.PolarDBXCluster) (StorageConnection, error) {
	var err error
	if polardb.Spec.Readonly {
		polardb, err = f.rc.GetPrimaryPolarDBX()
		if err != nil {
			return StorageConnection{}, err
		}
	}
	gmsObjectName := ""
	if polardb.Spec.ShareGMS {
		gmsObjectName = fmt.Sprintf("%s-%s-dn-0", polardb.Name, polardb.Status.Rand)
	} else {
		gmsObjectName = fmt.Sprintf("%s-%s-gms", polardb.Name, polardb.Status.Rand)
	}
	gmsSrv, err := f.rc.GetService(gmsObjectName)
	if err != nil {
		return StorageConnection{}, err
	}
	gmsSecret, err := f.rc.GetSecret(gmsObjectName)
	if err != nil {
		return StorageConnection{}, err
	}
	return StorageConnection{
		Host:     k8shelper.GetServiceDNSRecordWithSvc(gmsSrv, true),
		Port:     int(k8shelper.MustGetPortFromService(gmsSrv, "mysql").Port),
		User:     "admin",
		Passwd:   string(gmsSecret.Data["admin"]),
		Database: gms.MetaDBName,
	}, nil
}

func (f *objectFactory) newDeploymentUpgradeStrategy(polardbx *polardbxv1.PolarDBXCluster) appsv1.DeploymentStrategy {
	switch polardbx.Spec.UpgradeStrategy {
	case polardbxv1polardbx.RecreateUpgradeStrategy:
		return appsv1.DeploymentStrategy{
			Type: appsv1.RecreateDeploymentStrategyType,
		}
	case polardbxv1polardbx.RollingUpgradeStrategy:
		fallthrough
	default:
		half := intstr.FromString("50%")
		return appsv1.DeploymentStrategy{
			Type: appsv1.RollingUpdateDeploymentStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDeployment{
				MaxUnavailable: &half,
				MaxSurge:       &half,
			},
		}
	}
}

const (
	cnServerPostStartScript = `if [[ ! -f /usr/bin/myc ]]; then 
    echo 'mysql -h127.1 -P%d -upolardbx_root "$@"' > /usr/bin/myc && chmod +x /usr/bin/myc && ln -sf /usr/bin/myc /usr/bin/ctlocal;
	echo '%s "$@"' > /usr/bin/mya && chmod +x /usr/bin/mya && ln -sf /usr/bin/mya /usr/bin/ctmeta;
    echo 'mysql -h127.1 -P%d -upolardbx_root "$@"' > /usr/bin/mym && chmod +x /usr/bin/mym && ln -sf /usr/bin/mym /usr/bin/ctmgr;
fi

if [[ -d /home/admin/drds-worker ]]; then
	if [[ ! -L /usr/alisys/dragoon/bin/hblog_status ]]; then 
		touch /tmp/hblog_status; ln -sf /tmp/hblog_status /usr/alisys/dragoon/bin/hblog_status; 
	fi
	
	# Remove the global schedule script
	echo '' > /home/admin/drds-worker/bin/globalSchedule.sh
fi
`

	cnStartCmd = `
if [ -f /home/admin/entrypoint.sh ]; then
	sh /home/admin/entrypoint.sh
fi
if [ -f /home/admin/app.sh ]; then
	sh /home/admin/app.sh
fi
while [ "debug" == $(cat /etc/podinfo/runmode) ]
do
	echo "debug mode"
	sleep 3600
done
`
	cdcServerPostStartScript = `
if [[ -d /home/admin/drds-worker ]]; then
	# Remove the global schedule script
	echo '' > /home/admin/drds-worker/bin/globalSchedule.sh
fi
`
	cdcStartCmd = `
sh /home/admin/app.sh
while [ "debug" == $(cat /etc/podinfo/runmode) ]
do
	echo "debug mode"
	sleep 3600
done
`

	columnarServerPostStartScript = `if [[ ! -f /usr/bin/mya ]]; then 
	echo '%s "$@"' > /usr/bin/mya && chmod +x /usr/bin/mya && ln -sf /usr/bin/mya /usr/bin/ctmeta;
fi
`

	columnarStartCmd = `
sh /home/admin/app.sh
while [ "debug" == $(cat /etc/podinfo/runmode) ]
do
	echo "debug mode"
	sleep 3600
done
`
)

func (f *objectFactory) newDeployment4CN(group string, mr *matchingRule, mustStaticPorts bool) (*appsv1.Deployment, error) {
	polardbx := f.rc.MustGetPolarDBX()
	topology := polardbx.Status.SpecSnapshot.Topology
	template := polardbx.Status.SpecSnapshot.Topology.Nodes.CN.Template

	// Factories
	envFactory, err := NewEnvFactory(f.rc, polardbx, f)
	if err != nil {
		return nil, err
	}
	portsFactory := NewPortsFactory(f.rc, polardbx)
	volumeFactory := NewVolumeFactory(f.rc, polardbx)

	// Get GMS connection info.
	gmsConn, err := f.getGmsConn(polardbx)
	if err != nil {
		return nil, err
	}

	// Affinity
	var nodeSelector *corev1.NodeSelector
	if mr.rule != nil {
		nodeSelector, err = f.getNodeSelectorFromRef(polardbx, mr.rule.NodeSelector)
		if err != nil {
			return nil, err
		}
	}
	nodeSelector = f.updateNodeSelectorForExclusive(nodeSelector, polardbx.Spec.Exclusive)
	affinity := f.tryScatterAffinityForStatelessDeployment(
		convention.ConstLabelsWithRole(polardbx, polardbxmeta.RoleCN),
		nodeSelector,
	)

	// Name & Labels & Annotations
	deployName := convention.NewDeploymentName(polardbx, polardbxmeta.RoleCN, group)
	cnType := polardbxmeta.CNTypeRW
	if polardbx.Spec.Readonly {
		cnType = polardbxmeta.CNTypeRO
	}

	labels := convention.ConstLabelsForCN(polardbx, cnType)
	labels[polardbxmeta.LabelGroup] = group

	annotations := f.newPodAnnotations(polardbx)

	// Ports & Envs
	ports := portsFactory.NewPortsForCNEngine(mustStaticPorts)
	envVars := envFactory.NewEnvVarsForCNEngine(gmsConn, ports)

	// Host network
	podLabels := copyutil.CopyStrMap(labels)
	if template.HostNetwork {
		podLabels[polardbxmeta.LabelPortLock] = strconv.Itoa(ports.AccessPort)
		affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{
			{
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					polardbxmeta.LabelPortLock: strconv.Itoa(ports.AccessPort),
				}},
				Namespaces:  []string{polardbx.Namespace},
				TopologyKey: corev1.LabelHostname,
			},
		}
	}

	// Containers
	config := f.rc.Config()
	imageConfig := config.Images()

	// Container engine & prober
	probeConfigure := NewProbeConfigure(f.rc, polardbx)
	engineContainer := corev1.Container{
		Name: convention.ContainerEngine,
		Image: defaults.NonEmptyStrOrDefault(
			template.Image,
			imageConfig.DefaultImageForCluster(polardbxmeta.RoleCN, convention.ContainerEngine, topology.Version),
		),
		ImagePullPolicy: template.ImagePullPolicy,
		Env:             envVars,
		Resources:       *template.Resources.DeepCopy(),
		Ports: []corev1.ContainerPort{
			{Protocol: corev1.ProtocolTCP, Name: "mysql", ContainerPort: int32(ports.AccessPort)},
			{Protocol: corev1.ProtocolTCP, Name: "mgr", ContainerPort: int32(ports.MgrPort)},
			{Protocol: corev1.ProtocolTCP, Name: "mpp", ContainerPort: int32(ports.MppPort)},
			{Protocol: corev1.ProtocolTCP, Name: "htap", ContainerPort: int32(ports.HtapPort)},
			{Protocol: corev1.ProtocolTCP, Name: "log", ContainerPort: int32(ports.LogPort)},
		},
		Command:      []string{"/bin/bash", "-c"},
		Args:         []string{cnStartCmd},
		VolumeMounts: volumeFactory.NewVolumeMountsForCNEngine(),
		Lifecycle: &corev1.Lifecycle{
			PostStart: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"sudo", "/bin/bash", "-c", fmt.Sprintf(cnServerPostStartScript,
						ports.AccessPort,
						fmt.Sprintf("mysql -h%s -P%d -u%s -p%s -D%s", gmsConn.Host, gmsConn.Port, gmsConn.User, gmsConn.Passwd, gms.MetaDBName),
						ports.MgrPort,
					)},
				},
			},
		},
		SecurityContext: k8shelper.NewSecurityContext(config.Cluster().ContainerPrivileged()),
	}

	staticConfig := polardbx.Status.SpecSnapshot.Config.CN.Static
	if staticConfig != nil && staticConfig.EnableJvmRemoteDebug {
		engineContainer.Ports = append(engineContainer.Ports, corev1.ContainerPort{
			Protocol: corev1.ProtocolTCP, Name: "debug", ContainerPort: int32(ports.DebugPort),
		})
	}

	dnsPolicy := corev1.DNSClusterFirst
	if template.HostNetwork {
		dnsPolicy = corev1.DNSClusterFirstWithHostNet
	}

	probeConfigure.ConfigureForCNEngine(&engineContainer, ports)
	proberContainer := corev1.Container{
		Name:  convention.ContainerProber,
		Image: imageConfig.DefaultImageForCluster(polardbxmeta.RoleCN, convention.ContainerProber, topology.Version),
		Env: []corev1.EnvVar{
			{Name: "GOMAXPROCS", Value: "1"},
		},
		Args: []string{
			"--listen-port", fmt.Sprintf("%d", ports.ProbePort),
		},
		Ports: []corev1.ContainerPort{
			{Protocol: corev1.ProtocolTCP, Name: "probe", ContainerPort: int32(ports.ProbePort)},
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/liveness",
					Port: intstr.FromString("probe"),
					HTTPHeaders: []corev1.HTTPHeader{
						{Name: "Probe-Target", Value: probe.TypeSelf},
					},
				},
			},
		},
		VolumeMounts: volumeFactory.NewSystemVolumeMounts(),
	}
	if k8shelper.IsContainerQoSGuaranteed(&engineContainer) {
		if featuregate.EnforceQoSGuaranteed.Enabled() {
			proberContainer.Resources = corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}
		}
	}
	containers := []corev1.Container{engineContainer, proberContainer}

	// Container exporter if enabled
	if config.Cluster().EnableExporters() {
		exporterContainer := corev1.Container{
			Name:  convention.ContainerExporter,
			Image: imageConfig.DefaultImageForCluster(polardbxmeta.RoleCN, convention.ContainerExporter, topology.Version),
			Env: []corev1.EnvVar{
				{Name: "GOMAXPROCS", Value: "1"},
			},
			Args: []string{
				"-collectors.process",
				"-collectors.jvm",
				"-target.type=CN",
				fmt.Sprintf("-target.port=%d", ports.MgrPort),
				fmt.Sprintf("-web.listen-addr=:%d", ports.MetricsPort),
				"-web.metrics-path=/metrics",
			},
			VolumeMounts: volumeFactory.NewSystemVolumeMounts(),
			Ports: []corev1.ContainerPort{
				{Protocol: corev1.ProtocolTCP, Name: "metrics", ContainerPort: int32(ports.MetricsPort)},
			},
		}
		probeConfigure.ConfigureForCNExporter(&exporterContainer, ports)
		if k8shelper.IsContainerQoSGuaranteed(&engineContainer) {
			if featuregate.EnforceQoSGuaranteed.Enabled() {
				exporterContainer.Resources = corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("200m"),
						corev1.ResourceMemory: resource.MustParse("300Mi"),
					},
				}
			}
		}
		containers = append(containers, exporterContainer)
	}

	// Container init
	initContainer := corev1.Container{
		Name:  convention.ContainerInit,
		Image: imageConfig.DefaultImageForCluster(polardbxmeta.RoleCN, convention.ContainerInit, topology.Version),
		Env:   envVars,
	}
	if k8shelper.IsContainerQoSGuaranteed(&engineContainer) {
		if featuregate.EnforceQoSGuaranteed.Enabled() {
			initContainer.Resources = corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}
		}
	}

	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: f.rc.Namespace(),
			Labels: k8shelper.PatchLabels(
				copyutil.CopyStrMap(labels),
				map[string]string{
					polardbxmeta.LabelAuditLog: strconv.FormatBool(f.rc.MustGetPolarDBX().Spec.Config.CN.EnableAuditLog),
				},
			),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32(int32(mr.replicas)),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Strategy: f.newDeploymentUpgradeStrategy(polardbx),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: annotations,
					Finalizers:  []string{polardbxmeta.Finalizer},
				},
				Spec: corev1.PodSpec{
					// TODO: Add a config to control this switch
					EnableServiceLinks:            pointer.BoolPtr(false),
					ImagePullSecrets:              template.ImagePullSecrets,
					Volumes:                       volumeFactory.NewVolumesForCN(),
					InitContainers:                []corev1.Container{initContainer},
					Containers:                    containers,
					RestartPolicy:                 corev1.RestartPolicyAlways,
					TerminationGracePeriodSeconds: pointer.Int64(30),
					DNSPolicy:                     dnsPolicy,
					HostNetwork:                   template.HostNetwork,
					ShareProcessNamespace:         pointer.Bool(true),
					Affinity:                      affinity,
				},
			},
		},
	}

	//add hash to label
	convention.AddLabelHash(polardbxmeta.LabelHash, &deployment)
	//update generation label
	deployment.SetLabels(k8shelper.PatchLabels(deployment.Labels, map[string]string{
		polardbxmeta.LabelGeneration: strconv.FormatInt(polardbx.Status.ObservedGeneration, 10),
	}))

	// Return
	return &deployment, nil
}

func (f *objectFactory) NewDeployments4CN() (map[string]appsv1.Deployment, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}
	topology := polardbx.Status.SpecSnapshot.Topology
	rules := topology.Rules.Components.CN
	replicas, template := *topology.Nodes.CN.Replicas, topology.Nodes.CN.Template

	matchingRules, err := f.getStatelessMatchingRules(int(replicas), template.HostNetwork, rules)
	if err != nil {
		return nil, err
	}

	// Build deployments according rules.
	return f.buildDeployments(matchingRules, f.newDeployment4CN)
}

func (f *objectFactory) newDeployment4CDC(group string, mr *matchingRule, mustStaticPorts bool) (*appsv1.Deployment, error) {
	polardbx := f.rc.MustGetPolarDBX()
	config := f.rc.Config()
	topology := polardbx.Status.SpecSnapshot.Topology
	template := polardbx.Status.SpecSnapshot.Topology.Nodes.CDC.Template
	if f.buildContext.CdcGroup != nil && f.buildContext.CdcGroup.Template != nil {
		template = *f.buildContext.CdcGroup.Template
	}

	// Factories
	envFactory, err := NewEnvFactory(f.rc, polardbx, f)
	if err != nil {
		return nil, err
	}
	portsFactory := NewPortsFactory(f.rc, polardbx)
	volumeFactory := NewVolumeFactory(f.rc, polardbx)

	// Get GMS connection info.
	gmsConn, err := f.getGmsConn(polardbx)
	if err != nil {
		return nil, err
	}

	// Ports & Envs
	ports := portsFactory.NewPortsForCDCEngine()
	envVars := envFactory.NewEnvVarsForCDCEngine(gmsConn)

	// Affinity
	var nodeSelector *corev1.NodeSelector
	if mr.rule != nil {
		nodeSelector, err = f.getNodeSelectorFromRef(polardbx, mr.rule.NodeSelector)
		if err != nil {
			return nil, err
		}
	}
	nodeSelector = f.updateNodeSelectorForExclusive(nodeSelector, polardbx.Spec.Exclusive)
	affinity := f.tryScatterAffinityForStatelessDeployment(
		convention.ConstLabelsWithRole(polardbx, polardbxmeta.RoleCDC),
		nodeSelector,
	)

	// Name & Labels
	deployName := convention.NewDeploymentName(polardbx, polardbxmeta.RoleCDC, group)

	labels := convention.ConstLabelsWithRole(polardbx, polardbxmeta.RoleCDC)
	labels[polardbxmeta.LabelGroup] = group

	annotations := f.newPodAnnotations(polardbx)

	// Containers

	// Container engine & prober
	probeConfigure := NewProbeConfigure(f.rc, polardbx)
	engineContainer := corev1.Container{
		Name: convention.ContainerEngine,
		Image: defaults.NonEmptyStrOrDefault(
			template.Image,
			config.Images().DefaultImageForCluster(polardbxmeta.RoleCDC, convention.ContainerEngine, topology.Version),
		),
		Command:         []string{"/bin/bash", "-c"},
		Args:            []string{cdcStartCmd},
		ImagePullPolicy: template.ImagePullPolicy,
		Env:             envVars,
		Resources:       *template.Resources.DeepCopy(),
		Ports: []corev1.ContainerPort{
			{Protocol: corev1.ProtocolTCP, Name: "daemon", ContainerPort: int32(ports.DaemonPort)},
		},
		VolumeMounts: volumeFactory.NewVolumeMountsForCDCEngine(),
		Lifecycle: &corev1.Lifecycle{
			PostStart: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"sudo", "bash", "-c", cdcServerPostStartScript},
				},
			},
		},
		SecurityContext: k8shelper.NewSecurityContext(config.Cluster().ContainerPrivileged()),
	}
	probeConfigure.ConfigureForCDCEngine(&engineContainer, ports)
	proberContainer := corev1.Container{
		Name:  convention.ContainerProber,
		Image: config.Images().DefaultImageForCluster(polardbxmeta.RoleCDC, convention.ContainerProber, topology.Version),
		Env: []corev1.EnvVar{
			{Name: "GOMAXPROCS", Value: "1"},
		},
		Args: []string{
			"--listen-port", fmt.Sprintf("%d", ports.GetProbePort()),
		},
		Ports: []corev1.ContainerPort{
			{Protocol: corev1.ProtocolTCP, Name: "probe", ContainerPort: int32(ports.GetProbePort())},
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/liveness",
					Port: intstr.FromString("probe"),
					HTTPHeaders: []corev1.HTTPHeader{
						{Name: "Probe-Target", Value: probe.TypeSelf},
					},
				},
			},
		},
		VolumeMounts: volumeFactory.NewSystemVolumeMounts(),
	}
	if k8shelper.IsContainerQoSGuaranteed(&engineContainer) {
		if featuregate.EnforceQoSGuaranteed.Enabled() {
			proberContainer.Resources = corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}
		}
	}

	containers := []corev1.Container{engineContainer, proberContainer}

	dnsPolicy := corev1.DNSClusterFirst
	if template.HostNetwork {
		dnsPolicy = corev1.DNSClusterFirstWithHostNet
	}

	// Container exporter if enabled
	if config.Cluster().EnableExporters() {
		exporterContainer := corev1.Container{
			Name:  convention.ContainerExporter,
			Image: config.Images().DefaultImageForCluster(polardbxmeta.RoleCDC, convention.ContainerExporter, topology.Version),
			Env: []corev1.EnvVar{
				{Name: "GOMAXPROCS", Value: "1"},
			},
			Args: []string{
				fmt.Sprintf("-web.listen-addr=:%d", ports.MetricsPort),
				"-web.metrics-path=/metrics",
				fmt.Sprintf("-target.port=%d", ports.DaemonPort),
				"-target.type=CDC",
			},
			VolumeMounts: volumeFactory.NewSystemVolumeMounts(),
			Ports: []corev1.ContainerPort{
				{Protocol: corev1.ProtocolTCP, Name: "metrics", ContainerPort: int32(ports.MetricsPort)},
			},
		}
		probeConfigure.ConfigureForCDCExporter(&exporterContainer, ports)
		if k8shelper.IsContainerQoSGuaranteed(&engineContainer) {
			if featuregate.EnforceQoSGuaranteed.Enabled() {
				exporterContainer.Resources = corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("200m"),
						corev1.ResourceMemory: resource.MustParse("100Mi"),
					},
				}
			}
		}
		containers = append(containers, exporterContainer)
	}

	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: f.rc.Namespace(),
			Labels: k8shelper.PatchLabels(
				copyutil.CopyStrMap(labels),
				map[string]string{
					polardbxmeta.LabelGeneration: strconv.FormatInt(polardbx.Status.ObservedGeneration, 10),
				},
			),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32(int32(mr.replicas)),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Strategy: f.newDeploymentUpgradeStrategy(polardbx),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: annotations,
					Finalizers:  []string{polardbxmeta.Finalizer},
				},
				Spec: corev1.PodSpec{
					// TODO: Add a config to control this switch
					EnableServiceLinks:            pointer.BoolPtr(false),
					ImagePullSecrets:              template.ImagePullSecrets,
					Volumes:                       volumeFactory.NewVolumesForCDC(),
					Containers:                    containers,
					RestartPolicy:                 corev1.RestartPolicyAlways,
					TerminationGracePeriodSeconds: pointer.Int64(30),
					DNSPolicy:                     dnsPolicy,
					ShareProcessNamespace:         pointer.Bool(true),
					Affinity:                      affinity,
					// FIXME host network for CDC isn't supported
					// HostNetwork:                   template.HostNetwork,
				},
			},
		},
	}

	//add hash to label
	convention.AddLabelHash(polardbxmeta.LabelHash, &deployment)
	//update generation label
	deployment.SetLabels(k8shelper.PatchLabels(deployment.Labels, map[string]string{
		polardbxmeta.LabelGeneration: strconv.FormatInt(polardbx.Status.ObservedGeneration, 10),
	}))

	// Return
	return &deployment, nil
}

func (f *objectFactory) NewDeployments4Columnar() (map[string]appsv1.Deployment, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}
	topology := polardbx.Status.SpecSnapshot.Topology
	rules := topology.Rules.Components.Columnar
	nodes := topology.Nodes.Columnar
	if nodes == nil {
		return nil, nil
	}

	replicas := topology.Nodes.Columnar.Replicas
	matchingRules, err := f.getStatelessMatchingRules(int(replicas) /*template.HostNetwork*/, false, rules)
	if err != nil {
		return nil, err
	}

	// Build deployments according rules.
	return f.buildDeployments(matchingRules, f.newDeployment4Columnar)
}

func (f *objectFactory) newDeployment4Columnar(group string, mr *matchingRule, mustStaticPorts bool) (*appsv1.Deployment, error) {
	polardbx := f.rc.MustGetPolarDBX()
	config := f.rc.Config()
	topology := polardbx.Status.SpecSnapshot.Topology
	template := polardbx.Status.SpecSnapshot.Topology.Nodes.Columnar.Template

	// Factories
	envFactory, err := NewEnvFactory(f.rc, polardbx, f)
	if err != nil {
		return nil, err
	}
	//portsFactory := NewPortsFactory(f.rc, polardbx)
	volumeFactory := NewVolumeFactory(f.rc, polardbx)

	// Get GMS connection info.
	gmsConn, err := f.getGmsConn(polardbx)
	if err != nil {
		return nil, err
	}

	// Ports & Envs
	//ports := portsFactory.NewPortsForColumnarEngine()
	envVars := envFactory.NewEnvVarsForColumnarEngine(gmsConn)

	// Affinity
	var nodeSelector *corev1.NodeSelector
	if mr.rule != nil {
		nodeSelector, err = f.getNodeSelectorFromRef(polardbx, mr.rule.NodeSelector)
		if err != nil {
			return nil, err
		}
	}
	nodeSelector = f.updateNodeSelectorForExclusive(nodeSelector, polardbx.Spec.Exclusive)
	affinity := f.tryScatterAffinityForStatelessDeployment(
		convention.ConstLabelsWithRole(polardbx, polardbxmeta.RoleColumnar),
		nodeSelector,
	)

	// Name & Labels
	deployName := convention.NewDeploymentName(polardbx, polardbxmeta.RoleColumnar, group)

	labels := convention.ConstLabelsWithRole(polardbx, polardbxmeta.RoleColumnar)
	labels[polardbxmeta.LabelGroup] = group

	annotations := f.newPodAnnotations(polardbx)

	// Containers

	// Container engine
	engineContainer := corev1.Container{
		Name: convention.ContainerEngine,
		Image: defaults.NonEmptyStrOrDefault(
			template.Image,
			config.Images().DefaultImageForCluster(polardbxmeta.RoleColumnar, convention.ContainerEngine, topology.Version),
		),
		Command:         []string{"/bin/bash", "-c"},
		Args:            []string{columnarStartCmd},
		ImagePullPolicy: template.ImagePullPolicy,
		Env:             envVars,
		Resources:       *template.Resources.DeepCopy(),
		VolumeMounts:    volumeFactory.NewVolumeMountsForColumnarEngine(),
		SecurityContext: k8shelper.NewSecurityContext(config.Cluster().ContainerPrivileged()),
		Lifecycle: &corev1.Lifecycle{
			PostStart: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"sudo", "/bin/bash", "-c", fmt.Sprintf(columnarServerPostStartScript,
						fmt.Sprintf("mysql -h%s -P%d -u%s -p%s -D%s", gmsConn.Host, gmsConn.Port, gmsConn.User, gmsConn.Passwd, gms.MetaDBName),
					)},
				},
			},
		},
	}

	containers := []corev1.Container{engineContainer}

	dnsPolicy := corev1.DNSClusterFirst
	if template.HostNetwork {
		dnsPolicy = corev1.DNSClusterFirstWithHostNet
	}

	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: f.rc.Namespace(),
			Labels: k8shelper.PatchLabels(
				copyutil.CopyStrMap(labels),
				map[string]string{
					polardbxmeta.LabelGeneration: strconv.FormatInt(polardbx.Status.ObservedGeneration, 10),
				},
			),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32(int32(mr.replicas)),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Strategy: f.newDeploymentUpgradeStrategy(polardbx),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: annotations,
					Finalizers:  []string{polardbxmeta.Finalizer},
				},
				Spec: corev1.PodSpec{
					// TODO: Add a config to control this switch
					EnableServiceLinks:            pointer.BoolPtr(false),
					ImagePullSecrets:              template.ImagePullSecrets,
					Volumes:                       volumeFactory.NewVolumesForColumnar(),
					Containers:                    containers,
					RestartPolicy:                 corev1.RestartPolicyAlways,
					TerminationGracePeriodSeconds: pointer.Int64(30),
					DNSPolicy:                     dnsPolicy,
					ShareProcessNamespace:         pointer.Bool(true),
					Affinity:                      affinity,
					HostNetwork:                   template.HostNetwork,
				},
			},
		},
	}

	//add hash to label
	convention.AddLabelHash(polardbxmeta.LabelHash, &deployment)
	//update generation label
	deployment.SetLabels(k8shelper.PatchLabels(deployment.Labels, map[string]string{
		polardbxmeta.LabelGeneration: strconv.FormatInt(polardbx.Status.ObservedGeneration, 10),
	}))

	// Return
	return &deployment, nil
}

func (f *objectFactory) buildDeployments(rules map[string]matchingRule, builder func(name string, mr *matchingRule, mustStaticPorts bool) (*appsv1.Deployment, error)) (map[string]appsv1.Deployment, error) {
	deployments := make(map[string]appsv1.Deployment)
	for name, mr := range rules {
		deploy, err := builder(name, &mr, false)
		if err != nil {
			return nil, err
		}
		deploy2, err2 := builder(name, &mr, true)
		if err2 != nil {
			return nil, err2
		}
		deploy.SetLabels(k8shelper.PatchLabels(deploy.Labels, map[string]string{
			polardbxmeta.LabelHash: deploy2.Labels[polardbxmeta.LabelHash],
		}))
		deployments[name] = *deploy
	}
	return deployments, nil
}

func (f *objectFactory) filterRules(rules []polardbxv1polardbx.StatelessTopologyRuleItem, filter func(polardbxv1polardbx.StatelessTopologyRuleItem) bool) []polardbxv1polardbx.StatelessTopologyRuleItem {
	resultRules := make([]polardbxv1polardbx.StatelessTopologyRuleItem, 0)
	for _, rule := range rules {
		if filter(rule) {
			resultRules = append(resultRules, rule)
		}
	}
	return resultRules
}

func (f *objectFactory) NewDeployments4CDC() (map[string]appsv1.Deployment, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}
	topology := polardbx.Status.SpecSnapshot.Topology
	rules := topology.Rules.Components.CDC

	nodes := topology.Nodes.CDC
	if nodes == nil {
		return nil, nil
	}

	normalRules := f.filterRules(rules, func(item polardbxv1polardbx.StatelessTopologyRuleItem) bool {
		return item.ExtraName == ""
	})
	replicas := topology.Nodes.CDC.Replicas.IntValue() + topology.Nodes.CDC.XReplicas
	// FIXME Host network for CDC not supported.
	matchingRules, err := f.getStatelessMatchingRules(int(replicas) /*template.HostNetwork*/, false, normalRules)
	if err != nil {
		return nil, err
	}

	deployments := map[string]appsv1.Deployment{}
	if topology.Nodes.CDC.XReplicas > 0 {
		//first global binlog cdc, then binlog x cdc.
		resultMatchRules := SplitMatchRules(matchingRules, topology.Nodes.CDC.Replicas.IntValue(), topology.Nodes.CDC.XReplicas)
		buildXReplicasFlags := []bool{false, true}
		f.buildContext.HasCdcXBinLog = true
		for index, xReplicasFlags := range buildXReplicasFlags {
			f.buildContext.BuildCdcXBinLog = xReplicasFlags
			builtDeployments, err := f.buildDeployments(resultMatchRules[index], f.newDeployment4CDC)
			if err != nil {
				return deployments, err
			}
			deployments = maputil.MergeMap(deployments, builtDeployments, false).(map[string]appsv1.Deployment)
		}
		return deployments, nil
	} else {
		deployments, err = f.buildDeployments(matchingRules, f.newDeployment4CDC)
		if err != nil {
			return deployments, err
		}
	}

	for _, group := range topology.Nodes.CDC.Groups {
		f.buildContext.CdcGroup = group
		exactRules := f.filterRules(rules, func(item polardbxv1polardbx.StatelessTopologyRuleItem) bool {
			return item.ExtraName == group.Name
		})
		matchingRules, err = f.getStatelessMatchingRules(int(group.Replicas), false, exactRules)
		if err != nil {
			return nil, err
		}
		cdcGroupDeployments, err := f.buildDeployments(matchingRules, f.newDeployment4CDC)
		if err != nil {
			return cdcGroupDeployments, err
		}
		for k, v := range cdcGroupDeployments {
			deployments[k] = v
		}
		f.buildContext.CdcGroup = nil
	}
	return deployments, nil
}

func SplitMatchRules(matchRules map[string]matchingRule, replicasGroups ...int) (resultMatchRules []map[string]matchingRule) {
	resultMatchRules = make([]map[string]matchingRule, len(replicasGroups))
	for i := 0; i < len(replicasGroups); i++ {
		resultMatchRules[i] = map[string]matchingRule{}
	}
	replicaGroupIndex := 0

	ruleNames := dictutil.SortedStringKeys(matchRules)
	ruleNameIndex := 0
	for {
		ruleName := ruleNames[ruleNameIndex]
		matchRule := matchRules[ruleName]
		replicas := replicasGroups[replicaGroupIndex]
		minCount := math.MinInt(matchRule.replicas, replicas)
		resultMatchRules[replicaGroupIndex][fmt.Sprintf("%sg-%d", ruleName, replicaGroupIndex)] = matchingRule{
			replicas: minCount,
			rule:     matchRule.rule,
		}
		replicasGroups[replicaGroupIndex] = replicas - minCount
		if replicasGroups[replicaGroupIndex] == 0 {
			replicaGroupIndex++
		}
		matchRule.replicas = matchRule.replicas - minCount
		matchRules[ruleName] = matchRule
		if matchRule.replicas == 0 {
			ruleNameIndex++
		}
		if replicaGroupIndex >= len(replicasGroups) || ruleNameIndex >= len(ruleNames) {
			break
		}
	}
	return
}
func (f *objectFactory) updateNodeSelectorForExclusive(nodeSelector *corev1.NodeSelector, exclusive bool) *corev1.NodeSelector {
	if nodeSelector == nil {
		nodeSelector = &corev1.NodeSelector{}
	}
	nodeSelectorOp := corev1.NodeSelectorOpNotIn
	if exclusive {
		nodeSelectorOp = corev1.NodeSelectorOpIn
	}
	isolateCpuNodeSelectorRequirement := corev1.NodeSelectorRequirement{
		Key:      polardbxmeta.LabelIsolateCpu,
		Operator: nodeSelectorOp,
		Values:   []string{"true"},
	}
	nodeSelector = selector.AddNodeSelectorMatchExpressionRequirement(nodeSelector, isolateCpuNodeSelectorRequirement)
	return nodeSelector
}
