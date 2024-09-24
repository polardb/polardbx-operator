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

package polardbxcluster

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation/field"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1common "github.com/alibaba/polardbx-operator/api/v1/common"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	iniutil "github.com/alibaba/polardbx-operator/pkg/util/ini"
	"github.com/alibaba/polardbx-operator/pkg/webhook/extension"
)

func validateImagePullPolicy(fldPath *field.Path, policy corev1.PullPolicy) *field.Error {
	switch policy {
	case "", corev1.PullIfNotPresent, corev1.PullAlways, corev1.PullNever:
		return nil
	default:
		return field.NotSupported(fldPath, policy, []string{
			"", string(corev1.PullIfNotPresent), string(corev1.PullAlways), string(corev1.PullNever),
		})
	}
}

func validateResources(ctx context.Context, fldPath *field.Path, resources *corev1.ResourceRequirements) field.ErrorList {
	var errList field.ErrorList

	// If no limits and requests are set, there's a default value for the resource in CRD.
	if resources.Limits == nil && resources.Requests == nil {
		return nil
	}

	if resources.Limits == nil {
		errList = append(errList, field.Required(fldPath.Child("limits"), "limits is required"))
	}
	cpuLimits, cpuLimitsExists := resources.Limits[corev1.ResourceCPU]
	if !cpuLimitsExists {
		errList = append(errList, field.Required(fldPath.Child("limits", "cpu"), "limits.cpu is required"))
	}
	memoryLimits, memoryLimitsExists := resources.Limits[corev1.ResourceMemory]
	if !memoryLimitsExists {
		errList = append(errList, field.Required(fldPath.Child("limits", "memory"), "limits.memory is required"))
	}

	if resources.Requests != nil {
		if cpuLimitsExists {
			if cpuRequests, ok := resources.Requests[corev1.ResourceCPU]; ok {
				if cpuRequests.MilliValue() > cpuLimits.MilliValue() {
					errList = append(errList, field.Invalid(fldPath.Child("requests", "cpu"),
						cpuRequests.String(), "must be less than limits: "+cpuLimits.String()))
				}
			}
		}
		if memoryLimitsExists {
			if memoryRequests, ok := resources.Requests[corev1.ResourceMemory]; ok {
				if memoryRequests.Value() > memoryLimits.Value() {
					errList = append(errList, field.Invalid(fldPath.Child("requests", "memory"),
						memoryRequests.String(), "must be less than limits: "+memoryLimits.String()))
				}
			}
		}
	}

	return errList
}

func validateExtendResources(ctx context.Context, fldPath *field.Path, resources *polardbxv1common.ExtendedResourceRequirements) field.ErrorList {
	return validateResources(ctx, fldPath, &resources.ResourceRequirements)
}

type PolarDBXClusterV1Validator struct {
	configLoader func() *ValidatorConfig
}

func (v *PolarDBXClusterV1Validator) validateSecurity(ctx context.Context, security *polardbxv1polardbx.Security) field.ErrorList {
	var errList field.ErrorList
	if security == nil {
		return errList
	}

	if security.EncodeKey != nil {
		if security.EncodeKey.Name == "" {
			errList = append(errList, field.NotFound(
				field.NewPath("spec", "security", "encodeKey", "name"), ""),
			)
		}
		if security.EncodeKey.Key == "" {
			errList = append(errList, field.NotFound(
				field.NewPath("spec", "security", "encodeKey", "key"), ""),
			)
		}
	}

	return errList
}

func (v *PolarDBXClusterV1Validator) validateNodeSelector(fieldPath *field.Path, ns *corev1.NodeSelector) *field.Error {
	return nil
}

func (v *PolarDBXClusterV1Validator) validateStatelessTopologyRuleItems(ctx context.Context, fieldPath *field.Path, items []polardbxv1polardbx.StatelessTopologyRuleItem, validSelectors map[string]int) field.ErrorList {
	var errList field.ErrorList

	names := make(map[string]int)
	for index, item := range items {
		_, ok := names[item.Name]
		if ok {
			errList = append(errList, field.Duplicate(
				fieldPath.Index(index).Child("name"),
				item.Name),
			)
		} else {
			names[item.Name] = index
		}

		// Defer validation on replicas.

		ns := item.NodeSelector
		if ns != nil {
			if ns.Reference != "" {
				_, found := validSelectors[ns.Reference]
				if !found {
					errList = append(errList, field.Invalid(
						fieldPath.Index(index).Child("selector", "reference"),
						ns.Reference,
						"invalid selector, not predefined",
					))
				}
			}
		}
	}

	return errList
}

func (v *PolarDBXClusterV1Validator) validateXStoreTopologyRule(ctx context.Context, fieldPath *field.Path, rule *polardbxv1polardbx.XStoreTopologyRule, validSelectors map[string]int) field.ErrorList {
	var errList field.ErrorList

	if rule == nil {
		return errList
	}

	if rule.Rolling != nil && len(rule.NodeSets) > 0 {
		errList = append(errList, field.Invalid(fieldPath,
			rule,
			"rolling and nodeSets can not be both defined"))
		return errList
	}

	if rule.Rolling != nil {
		if rule.Rolling.Replicas%2 == 0 {
			errList = append(errList, field.Invalid(
				fieldPath.Child("rolling", "replicas"),
				rule.Rolling.Replicas,
				"must be odd"))
		}

		ns := rule.Rolling.NodeSelector
		if ns != nil {
			if ns.Reference != "" {
				_, found := validSelectors[ns.Reference]
				if !found {
					errList = append(errList, field.Invalid(
						fieldPath.Child("rolling", "selector", "reference"),
						ns.Reference,
						"invalid selector, not predefined",
					))
				}
			}
		}
	}

	if len(rule.NodeSets) > 0 {
		nodeSetNames := make(map[string]int)
		for index, nodeSet := range rule.NodeSets {
			if nodeSet.Name == "" {
				errList = append(errList, field.NotFound(
					fieldPath.Child("nodeSets").Index(index).Child("name"),
					"",
				))
			} else {
				_, found := nodeSetNames[nodeSet.Name]
				if found {
					errList = append(errList, field.Duplicate(
						fieldPath.Child("nodeSets").Index(index).Child("name"),
						nodeSet.Name))
				} else {
					nodeSetNames[nodeSet.Name] = index
				}
			}

			switch nodeSet.Role {
			case polardbxv1xstore.RoleCandidate,
				polardbxv1xstore.RoleVoter,
				polardbxv1xstore.RoleLearner:
				// break
			default:
				errList = append(errList, field.NotSupported(
					fieldPath.Child("nodeSets").Index(index).Child("role"),
					nodeSet.Role,
					[]string{
						string(polardbxv1xstore.RoleCandidate),
						string(polardbxv1xstore.RoleVoter),
						string(polardbxv1xstore.RoleLearner),
					}),
				)
			}

			ns := nodeSet.NodeSelector
			if ns != nil {
				if ns.Reference != "" {
					_, found := validSelectors[ns.Reference]
					if !found {
						errList = append(errList, field.Invalid(
							fieldPath.Index(index).Child("selector", "reference"),
							ns.Reference,
							"invalid selector, not predefined",
						))
					}
				}
			}
		}
	}

	return errList
}

func (v *PolarDBXClusterV1Validator) validateTopologyRules(ctx context.Context, rules *polardbxv1polardbx.TopologyRules) field.ErrorList {
	var errList field.ErrorList
	fieldPath := field.NewPath("spec", "topology", "rules")

	validSelectors := make(map[string]int)
	for index, selector := range rules.Selectors {
		if selector.Name == "" {
			errList = append(errList, field.NotFound(
				fieldPath.
					Child("selectors").
					Index(index).
					Child("name"),
				""),
			)
		} else {
			_, ok := validSelectors[selector.Name]
			if ok {
				errList = append(errList, field.Duplicate(
					fieldPath.
						Child("selectors").
						Index(index).
						Child("name"),
					selector.Name),
				)
			} else {
				validSelectors[selector.Name] = index
			}
		}

		err := v.validateNodeSelector(fieldPath.
			Child("selectors").
			Index(index).
			Child("nodeSelector"), &selector.NodeSelector)
		if err != nil {
			errList = append(errList, err)
		}
	}

	errList = append(errList, v.validateStatelessTopologyRuleItems(ctx,
		fieldPath.Child("components", "cn"), rules.Components.CN, validSelectors)...)
	errList = append(errList, v.validateStatelessTopologyRuleItems(ctx,
		fieldPath.Child("components", "cdc"), rules.Components.CDC, validSelectors)...)
	errList = append(errList, v.validateStatelessTopologyRuleItems(ctx,
		fieldPath.Child("components", "columnar"), rules.Components.Columnar, validSelectors)...)
	errList = append(errList, v.validateXStoreTopologyRule(ctx,
		fieldPath.Child("components", "gms"), rules.Components.GMS, validSelectors)...)
	errList = append(errList, v.validateXStoreTopologyRule(ctx,
		fieldPath.Child("components", "dn"), rules.Components.DN, validSelectors)...)

	return errList
}

func (v *PolarDBXClusterV1Validator) validateXStoreTemplate(ctx context.Context, fldPath *field.Path, template *polardbxv1polardbx.XStoreTemplate) field.ErrorList {
	if template == nil {
		return nil
	}

	var errList field.ErrorList

	switch template.ServiceType {
	case corev1.ServiceTypeClusterIP,
		corev1.ServiceTypeNodePort,
		corev1.ServiceTypeLoadBalancer,
		corev1.ServiceTypeExternalName: // break
	default:
		errList = append(errList, field.NotSupported(
			fldPath.Child("serviceType"),
			template.ServiceType,
			[]string{
				string(corev1.ServiceTypeClusterIP),
				string(corev1.ServiceTypeNodePort),
				string(corev1.ServiceTypeLoadBalancer),
				string(corev1.ServiceTypeExternalName),
			}))
	}

	if err := validateImagePullPolicy(fldPath.Child("imagePullPolicy"), template.ImagePullPolicy); err != nil {
		errList = append(errList, err)
	}

	errList = append(errList, validateExtendResources(ctx, fldPath.Child("resources"), &template.Resources)...)

	return errList
}

func (v *PolarDBXClusterV1Validator) validateCNTemplate(ctx context.Context, fldPath *field.Path, template *polardbxv1polardbx.CNTemplate) field.ErrorList {
	var errList field.ErrorList

	errList = append(errList, validateResources(ctx, fldPath.Child("resources"), &template.Resources)...)

	if err := validateImagePullPolicy(fldPath.Child("imagePullPolicy"), template.ImagePullPolicy); err != nil {
		errList = append(errList, err)
	}

	return errList
}

func (v *PolarDBXClusterV1Validator) validateCDCTemplate(ctx context.Context, fldPath *field.Path, template *polardbxv1polardbx.CDCTemplate) field.ErrorList {
	var errList field.ErrorList

	errList = append(errList, validateResources(ctx, fldPath.Child("resources"), &template.Resources)...)

	if err := validateImagePullPolicy(fldPath.Child("imagePullPolicy"), template.ImagePullPolicy); err != nil {
		errList = append(errList, err)
	}

	return errList
}

func (v *PolarDBXClusterV1Validator) validateColumnarTemplate(ctx context.Context, fldPath *field.Path, template *polardbxv1polardbx.ColumnarTemplate) field.ErrorList {
	var errList field.ErrorList

	errList = append(errList, validateResources(ctx, fldPath.Child("resources"), &template.Resources)...)

	if err := validateImagePullPolicy(fldPath.Child("imagePullPolicy"), template.ImagePullPolicy); err != nil {
		errList = append(errList, err)
	}

	return errList
}

func (v *PolarDBXClusterV1Validator) validateTopologyNodes(ctx context.Context, nodes *polardbxv1polardbx.TopologyNodes) field.ErrorList {
	var errList field.ErrorList
	fldPath := field.NewPath("spec", "topology", "nodes")
	errList = append(errList, v.validateXStoreTemplate(ctx,
		fldPath.Child("gms", "template"),
		nodes.GMS.Template)...)
	errList = append(errList, v.validateXStoreTemplate(ctx,
		fldPath.Child("dn", "template"),
		&nodes.DN.Template)...)
	errList = append(errList, v.validateCNTemplate(ctx,
		fldPath.Child("cn", "template"),
		&nodes.CN.Template)...)
	if nodes.CDC != nil {
		errList = append(errList, v.validateCDCTemplate(ctx,
			fldPath.Child("cdc", "template"),
			&nodes.CDC.Template)...)
	}
	if nodes.Columnar != nil {
		errList = append(errList, v.validateColumnarTemplate(ctx,
			fldPath.Child("columnar", "template"),
			&nodes.Columnar.Template)...)
	}
	return errList
}

func getRuleReplicas(total int, replicas *intstr.IntOrString) (int, error) {
	if replicas.Type == intstr.Int {
		val := replicas.IntValue()
		return val, nil
	} else {
		s := replicas.StrVal
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
			val, err := strconv.Atoi(replicas.StrVal)
			if err != nil {
				return 0, fmt.Errorf("invalid replicas: %w", err)
			}
			return val, nil
		}
	}
}

func (v *PolarDBXClusterV1Validator) validateReplicasInRule(fldPath *field.Path, replicas *intstr.IntOrString, total int) (int, *field.Error) {
	r, err := getRuleReplicas(total, replicas)
	if err != nil {
		return 0, field.Invalid(fldPath, replicas, err.Error())
	}
	return r, nil
}

func (v *PolarDBXClusterV1Validator) validateReplicasOnStatelessComponent(ctx context.Context, rules []polardbxv1polardbx.StatelessTopologyRuleItem, fldPath *field.Path, replicas int) field.ErrorList {
	var errList field.ErrorList

	sum := 0
	emptyReplicas := 0
	for i, rule := range rules {
		rFldPath := fldPath.Index(i).Child("replicas")
		if rule.Replicas == nil {
			emptyReplicas++
			if emptyReplicas > 1 {
				errList = append(errList, field.Forbidden(rFldPath, "multiple nil replicas found, only one is allowed"))
			}
			continue
		} else {
			r, err := v.validateReplicasInRule(rFldPath, rule.Replicas, replicas)
			if err != nil {
				errList = append(errList, err)
			}
			sum += r
		}
	}

	// It's allowed to be less or equal.
	if sum > replicas {
		errList = append(errList, field.Forbidden(fldPath,
			fmt.Sprintf("invalid rules, sum of replicas %d is larger than declared %d", sum, replicas)))
	}

	return errList
}

func (v *PolarDBXClusterV1Validator) validateReplicas(ctx context.Context, topology *polardbxv1polardbx.Topology) field.ErrorList {
	var errList field.ErrorList

	cnRules := topology.Rules.Components.CN
	cnNodes := topology.Nodes.CN
	// Skip if CN nodes is nil.
	errList = append(errList, v.validateReplicasOnStatelessComponent(ctx, cnRules,
		field.NewPath("spec", "topology", "rules", "cn"), int(*cnNodes.Replicas))...)

	cdcRules := topology.Rules.Components.CDC
	cdcNodes := topology.Nodes.CDC
	// Skip if CDC nodes is nil.
	if cdcNodes != nil {
		errList = append(errList, v.validateReplicasOnStatelessComponent(ctx, cdcRules,
			field.NewPath("spec", "topology", "rules", "cdc"), int(cdcNodes.Replicas.IntValue()+cdcNodes.XReplicas))...)
	}

	columnarRules := topology.Rules.Components.Columnar
	columnarNodes := topology.Nodes.Columnar
	// Skip if Columnar nodes is nil.
	if columnarNodes != nil {
		errList = append(errList, v.validateReplicasOnStatelessComponent(ctx, columnarRules,
			field.NewPath("spec", "topology", "rules", "columnar"), int(columnarNodes.Replicas))...)
	}

	return errList
}

func (v *PolarDBXClusterV1Validator) validateTopology(ctx context.Context, topology *polardbxv1polardbx.Topology) field.ErrorList {
	var errList field.ErrorList

	errList = append(errList, v.validateTopologyRules(ctx, &topology.Rules)...)
	errList = append(errList, v.validateTopologyNodes(ctx, &topology.Nodes)...)
	errList = append(errList, v.validateReplicas(ctx, topology)...)

	return errList
}

func (v *PolarDBXClusterV1Validator) validatePrivileges(ctx context.Context, privileges []polardbxv1polardbx.PrivilegeItem) field.ErrorList {
	var errList field.ErrorList

	usernames := make(map[string]int)
	fieldPath := field.NewPath("spec", "privileges")
	for index, priv := range privileges {
		if priv.Username == "" {
			errList = append(errList, field.NotFound(
				fieldPath.Index(index).Child("username"),
				""),
			)
		} else {
			_, ok := usernames[priv.Username]
			if ok {
				errList = append(errList, field.Duplicate(
					fieldPath.Index(index).Child("username"),
					priv.Username),
				)
			} else {
				usernames[priv.Username] = index
			}
		}

		switch priv.Type {
		case polardbxv1polardbx.ReadWrite, polardbxv1polardbx.ReadOnly,
			polardbxv1polardbx.DDLOnly, polardbxv1polardbx.DMLOnly,
			polardbxv1polardbx.Super, "":
			// break
		default:
			errList = append(errList, field.NotSupported(
				fieldPath.Index(index).Child("type"),
				priv.Type,
				[]string{
					string(polardbxv1polardbx.ReadWrite),
					string(polardbxv1polardbx.ReadOnly),
					string(polardbxv1polardbx.DDLOnly),
					string(polardbxv1polardbx.DMLOnly),
					string(polardbxv1polardbx.Super),
				}),
			)
		}
	}

	return errList
}

func (v *PolarDBXClusterV1Validator) validateConfig(ctx context.Context, config *polardbxv1polardbx.Config) field.ErrorList {
	var errList field.ErrorList
	fieldPath := field.NewPath("spec", "config")

	// Check DN's overwrite.
	if config.DN.MycnfOverwrite != "" {
		_, err := iniutil.ParseMyCnfOverlayFile(bytes.NewBufferString(config.DN.MycnfOverwrite))
		if err != nil {
			errList = append(errList, field.Invalid(
				fieldPath.Child("config", "dn", "mycnfOverwrite"),
				config.DN.MycnfOverwrite,
				"invalid format, parse error: "+err.Error()),
			)
		}
	}

	return errList
}

func (v *PolarDBXClusterV1Validator) validate(ctx context.Context, polardbx *polardbxv1.PolarDBXCluster) error {
	spec := &polardbx.Spec

	errList := field.ErrorList{}

	errList = append(errList, v.validateTopology(ctx, &polardbx.Spec.Topology)...)
	errList = append(errList, v.validateConfig(ctx, &polardbx.Spec.Config)...)
	errList = append(errList, v.validatePrivileges(ctx, polardbx.Spec.Privileges)...)
	errList = append(errList, v.validateSecurity(ctx, polardbx.Spec.Security)...)

	if spec.Readonly {
		if spec.PrimaryCluster == "" {
			errList = append(errList, field.Invalid(
				field.NewPath("spec", "primaryCluster"),
				spec.PrimaryCluster,
				`primaryCluster cannot be empty for readonly pxc`,
			))
		}
	}

	switch spec.ProtocolVersion.String() {
	case "5", "5.7", "8", "8.0": // break
	default:
		errList = append(errList, field.NotSupported(
			field.NewPath("spec", "protocolVersion"),
			spec.ProtocolVersion,
			[]string{
				"5", "5.7", "8", "8.0",
			}),
		)
	}

	staticConfig := spec.Config.CN.Static
	if staticConfig != nil {
		switch staticConfig.RPCProtocolVersion.String() {
		case "1", "2", "":
		default:
			errList = append(errList, field.NotSupported(
				field.NewPath("spec", "config", "cn", "static", "RPCProtocolVersion"),
				spec.Config.CN.Static.RPCProtocolVersion,
				[]string{
					"1", "2",
				}),
			)
		}
	}

	switch spec.ServiceType {
	case corev1.ServiceTypeClusterIP,
		corev1.ServiceTypeNodePort,
		corev1.ServiceTypeLoadBalancer,
		corev1.ServiceTypeExternalName: // break
	default:
		errList = append(errList, field.NotSupported(
			field.NewPath("spec", "serviceType"),
			spec.ServiceType,
			[]string{
				string(corev1.ServiceTypeClusterIP),
				string(corev1.ServiceTypeNodePort),
				string(corev1.ServiceTypeLoadBalancer),
				string(corev1.ServiceTypeExternalName),
			}),
		)
	}

	switch spec.UpgradeStrategy {
	case polardbxv1polardbx.RecreateUpgradeStrategy,
		polardbxv1polardbx.RollingUpgradeStrategy: // break
	default:
		errList = append(errList, field.NotSupported(
			field.NewPath("spec", "upgradeStrategy"),
			spec.UpgradeStrategy,
			[]string{
				string(polardbxv1polardbx.RecreateUpgradeStrategy),
				string(polardbxv1polardbx.RollingUpgradeStrategy),
			}))
	}

	if spec.Topology.Nodes.CDC != nil {
		cdcGroups := spec.Topology.Nodes.CDC.Groups
		if cdcGroups != nil {
			uniqueMap := map[string]bool{}
			for _, cdcGroup := range cdcGroups {
				if _, ok := uniqueMap[cdcGroup.Name]; ok {
					errList = append(errList, field.Duplicate(field.NewPath("spec.Topology.Nodes.CDC.Groups[].name"), cdcGroup.Name))
					break
				}
				uniqueMap[cdcGroup.Name] = true
			}
		}
	}

	if spec.Exclusive {
		topologyNodes := spec.Topology.Nodes
		if topologyNodes.CDC != nil && !v.checkCpuResourceGuaranteed(topologyNodes.CDC.Template.Resources) {
			errList = append(errList, field.Forbidden(field.NewPath("spec.Topology.Nodes.CDC.Template.Resources"), "cpu request and limit must be equal"))
		}
		if topologyNodes.Columnar != nil && !v.checkCpuResourceGuaranteed(topologyNodes.Columnar.Template.Resources) {
			errList = append(errList, field.Forbidden(field.NewPath("spec.Topology.Nodes.Columnar.Template.Resources"), "cpu request and limit must be equal"))
		}
		if !v.checkCpuResourceGuaranteed(topologyNodes.CN.Template.Resources) {
			errList = append(errList, field.Forbidden(field.NewPath("spec.Topology.Nodes.CN.Template.Resources"), "cpu request and limit must be equal"))
		}
		if !v.checkCpuResourceGuaranteed(topologyNodes.DN.Template.Resources.ResourceRequirements) {
			errList = append(errList, field.Forbidden(field.NewPath("spec.Topology.Nodes.DN.Template.Resources"), "cpu request and limit must be equal"))
		}
		if topologyNodes.GMS.Template != nil && !v.checkCpuResourceGuaranteed(topologyNodes.GMS.Template.Resources.ResourceRequirements) {
			errList = append(errList, field.Forbidden(field.NewPath("spec.Topology.Nodes.GMS.Template.Resources"), "cpu request and limit must be equal"))
		}
	}

	if len(errList) > 0 {
		return apierrors.NewInvalid(
			polardbx.GroupVersionKind().GroupKind(),
			polardbx.Name,
			errList)
	}
	return nil
}

func (v *PolarDBXClusterV1Validator) checkCpuResourceGuaranteed(resources corev1.ResourceRequirements) bool {
	if resources.Requests != nil && resources.Requests.Cpu() != nil && resources.Limits != nil && resources.Limits.Cpu() != nil {
		return equality.Semantic.DeepEqual(resources.Requests.Cpu(), resources.Limits.Cpu())
	}
	return true
}

func (v *PolarDBXClusterV1Validator) ValidateCreate(ctx context.Context, obj runtime.Object) error {
	return v.validate(ctx, obj.(*polardbxv1.PolarDBXCluster))
}

func (v *PolarDBXClusterV1Validator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) error {
	old, new := oldObj.(*polardbxv1.PolarDBXCluster), newObj.(*polardbxv1.PolarDBXCluster)
	gvk := old.GroupVersionKind()

	// If syncing spec from backup set, skip validating update
	if old.Status.Phase == polardbxv1polardbx.PhasePending && old.Spec.Restore != nil && old.Spec.Restore.SyncSpecWithOriginalCluster {
		return nil
	}

	// Validate the immutable fields, such as storage engine.
	oldSpec, newSpec := &old.Spec, &new.Spec

	if oldSpec.ShareGMS != newSpec.ShareGMS {
		return apierrors.NewForbidden(
			schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			},
			new.Name,
			field.Forbidden(field.NewPath("spec").Child("shareGMS"), "field is immutable"),
		)
	}

	if oldSpec.Readonly != newSpec.Readonly {
		return apierrors.NewForbidden(
			schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			},
			new.Name,
			field.Forbidden(field.NewPath("spec").Child("readonly"), "field is immutable"),
		)
	}

	if oldSpec.PrimaryCluster != newSpec.PrimaryCluster {
		return apierrors.NewForbidden(
			schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			},
			new.Name,
			field.Forbidden(field.NewPath("spec").Child("primaryCluster"), "field is immutable"),
		)
	}

	if oldSpec.TDE.Enable == true && oldSpec.TDE.Enable != newSpec.TDE.Enable {
		return apierrors.NewForbidden(
			schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			},
			new.Name,
			field.Forbidden(field.NewPath("spec").Child("tde").Child("enable"), "tde can not be closed"),
		)
	}

	if oldSpec.TDE.Enable == true && oldSpec.TDE.KeyringPath != newSpec.TDE.KeyringPath {
		return apierrors.NewForbidden(
			schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			},
			new.Name,
			field.Forbidden(field.NewPath("spec").Child("tde").Child("keyringPath"), "keyringPath can not be changed when tde open"),
		)
	}

	oldStorageEngine := oldSpec.Topology.Nodes.DN.Template.Engine
	newStorageEngine := newSpec.Topology.Nodes.DN.Template.Engine
	if oldStorageEngine != newStorageEngine {
		return apierrors.NewForbidden(
			schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			},
			new.Name,
			field.Forbidden(field.NewPath("spec").
				Child("topology").
				Child("nodes").
				Child("dn").
				Child("template").
				Child("engine"),
				"storage engine can not be changed"),
		)
	}

	oldGmsStorageEngine := oldStorageEngine
	if oldSpec.Topology.Nodes.GMS.Template != nil {
		oldGmsStorageEngine = oldSpec.Topology.Nodes.GMS.Template.Engine
	}
	newGmsStorageEngine := newStorageEngine
	if newSpec.Topology.Nodes.GMS.Template != nil {
		newGmsStorageEngine = newSpec.Topology.Nodes.GMS.Template.Engine
	}
	if oldGmsStorageEngine != newGmsStorageEngine {
		return apierrors.NewForbidden(
			schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			},
			new.Name,
			field.Forbidden(field.NewPath("spec").
				Child("topology").
				Child("nodes").
				Child("gms").
				Child("template").
				Child("engine"),
				"storage engine can not be changed"),
		)
	}

	if !equality.Semantic.DeepEqual(oldSpec.Security, newSpec.Security) {
		return apierrors.NewForbidden(
			schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			},
			new.Name,
			field.Forbidden(
				field.NewPath("spec", "security"),
				"security is immutable",
			),
		)
	}

	if !equality.Semantic.DeepEqual(oldSpec.InitReadonly, newSpec.InitReadonly) {
		return apierrors.NewForbidden(
			schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			},
			new.Name,
			field.Forbidden(
				field.NewPath("spec", "initReadonly"),
				"initReadonly is immutable",
			),
		)
	}

	if !equality.Semantic.DeepEqual(oldSpec.Config.CN.ColdDataFileStorage, newSpec.Config.CN.ColdDataFileStorage) {
		return apierrors.NewForbidden(
			schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			},
			new.Name,
			field.Forbidden(
				field.NewPath("spec", "config", "cn", "coldDataFileStorage"),
				"coldDataFileStorage is immutable",
			),
		)
	}

	if !equality.Semantic.DeepEqual(oldSpec.Exclusive, newSpec.Exclusive) {
		return apierrors.NewForbidden(
			schema.GroupResource{
				Group:    gvk.Group,
				Resource: gvk.Kind,
			},
			new.Name,
			field.Forbidden(
				field.NewPath("spec", "exclusive"),
				"Exclusive is immutable",
			),
		)
	}

	// Validate the new object at last.
	return v.validate(ctx, new)
}

func (v *PolarDBXClusterV1Validator) ValidateDelete(ctx context.Context, obj runtime.Object) error {
	return nil
}

func NewPolarDBXClusterV1Validator(configLoader func() *ValidatorConfig) extension.CustomValidator {
	return &PolarDBXClusterV1Validator{
		configLoader: configLoader,
	}
}
