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
	"strconv"
	"strings"

	"github.com/alibaba/polardbx-operator/pkg/util/formula"

	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	iniutil "github.com/alibaba/polardbx-operator/pkg/util/ini"
	"gopkg.in/ini.v1"

	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1common "github.com/alibaba/polardbx-operator/api/v1/common"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func readValueFrom(rc *reconcile.Context, valueFrom *polardbxv1common.ValueFrom) (string, error) {
	if valueFrom == nil {
		return "", nil
	}
	if valueFrom.ConfigMap != nil {
		cm, err := rc.GetConfigMap(valueFrom.ConfigMap.Name)
		if err != nil {
			return "", err
		}
		return cm.Data[valueFrom.ConfigMap.Key], nil
	}
	return "", nil
}

func readValue(rc *reconcile.Context, value *polardbxv1common.Value) (string, error) {
	if value == nil {
		return "", nil
	}
	if value.Value != nil {
		return *value.Value, nil
	}
	if value.ValueFrom != nil {
		return readValueFrom(rc, value.ValueFrom)
	}
	return "", nil
}

func fakeIniWithDefaultSectionIfNoSectionAtStart(s string, section string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "[") {
		return s
	}
	return "[" + section + "]\n" + s
}

func setValueTemplate(param polardbxv1.TemplateParams, mycnfOverrride *ini.File, xstore *polardbxv1.XStore) error {
	if len(param.DefaultValue) > 0 && param.DefaultValue[0] == '{' {
		calculateValue, err := formula.FormulaComputingXStore(param.DefaultValue, xstore)
		if err != nil {
			return err
		}
		mycnfOverrride.Section("mysqld").Key(param.Name).SetValue(strconv.Itoa(calculateValue))
	} else {
		mycnfOverrride.Section("mysqld").Key(param.Name).SetValue(param.DefaultValue)
	}
	return nil
}

func setValue(param polardbxv1.Params, mycnfOverrride *ini.File, xstore *polardbxv1.XStore) {
	if len(param.Value) > 0 && param.Value[0] == '{' {
		calculateValue, _ := formula.FormulaComputingXStore(param.Value, xstore)
		mycnfOverrride.Section("mysqld").Key(param.Name).SetValue(strconv.Itoa(calculateValue))
	} else {
		mycnfOverrride.Section("mysqld").Key(param.Name).SetValue(param.Value)
	}
}

func setMycnfTemplate(keys []*ini.Key, templateParamList []polardbxv1.TemplateParams, paramList []polardbxv1.Params,
	mycnfOverrride *ini.File, paramsRoleMap map[string]map[string]polardbxv1.Params, role string, xstore *polardbxv1.XStore) {
	// template parameters
	for _, param := range templateParamList {
		exists := false
		for _, key := range keys {
			if key.Name() == param.Name {
				exists = true
				break
			}
		}
		if !exists {
			if role == polardbxmeta.RoleDN {
				if _, ok := paramsRoleMap[polardbxv1.DNReadOnly][param.Name]; !ok {
					setValueTemplate(param, mycnfOverrride, xstore)
				}
			} else {
				if _, ok := paramsRoleMap[polardbxv1.GMSReadOnly][param.Name]; !ok {
					setValueTemplate(param, mycnfOverrride, xstore)
				}
			}
		}
	}
	// dynamic parameters
	for _, param := range paramList {
		exists := false
		for _, key := range keys {
			if key.Name() == param.Name {
				exists = true
				break
			}
		}
		if !exists {
			if role == polardbxmeta.RoleDN {
				if _, ok := paramsRoleMap[polardbxv1.DNReadOnly][param.Name]; !ok {
					setValue(param, mycnfOverrride, xstore)
				}
			} else {
				if _, ok := paramsRoleMap[polardbxv1.GMSReadOnly][param.Name]; !ok {
					setValue(param, mycnfOverrride, xstore)
				}
			}
		}
	}
}

func syncParameterToConfigMap(rc *reconcile.Context, xstore *polardbxv1.XStore, overrideVal string) (string, error) {
	mycnfTemplate, err := ini.LoadSources(ini.LoadOptions{
		AllowBooleanKeys:           true,
		AllowPythonMultilineValues: true,
		SpaceBeforeInlineComment:   true,
		PreserveSurroundedQuote:    true,
		IgnoreInlineComment:        true,
	}, []byte(overrideVal))
	if err != nil {
		return "", err
	}

	keys := mycnfTemplate.Section("mysqld").Keys()

	cmRole := xstore.Labels[polardbxmeta.LabelRole]

	parameterTemplateName := rc.GetPolarDBXTemplateName()
	parameterTemplateNameSpace := rc.GetPolarDBXTemplateNameSpace()

	if parameterTemplateName == "" {
		return overrideVal, nil
	}

	pt, err := rc.GetPolarDBXParameterTemplate(parameterTemplateNameSpace, parameterTemplateName)
	if pt == nil {
		return "", err
	}

	if cmRole == polardbxmeta.RoleGMS && len(pt.Spec.NodeType.GMS.ParamList) == 0 {
		cmRole = polardbxmeta.RoleDN
	}

	polarDBXParameterList := polardbxv1.PolarDBXParameterList{}
	err = rc.Client().List(rc.Context(), &polarDBXParameterList,
		client.InNamespace(rc.Namespace()),
		client.MatchingLabels(convention.GetParameterLabel()),
	)
	if err != nil {
		return "", err
	}
	var parameter polardbxv1.PolarDBXParameter
	for _, p := range polarDBXParameterList.Items {
		if xstore.Labels[polardbxmeta.LabelName] == p.Spec.ClusterName && xstore.Spec.ParameterTemplate.Name == p.Spec.TemplateName {
			parameter = p
			break
		}
	}

	paramsRoleMap := rc.GetPolarDBXParams()
	if cmRole == polardbxmeta.RoleDN {
		setMycnfTemplate(keys, pt.Spec.NodeType.DN.ParamList, parameter.Spec.NodeType.DN.ParamList, mycnfTemplate, paramsRoleMap, polardbxmeta.RoleDN, xstore)
	} else {
		setMycnfTemplate(keys, pt.Spec.NodeType.GMS.ParamList, parameter.Spec.NodeType.GMS.ParamList, mycnfTemplate, paramsRoleMap, polardbxmeta.RoleGMS, xstore)
	}

	return iniutil.ToString(mycnfTemplate), nil
}

func newConfigDataMap(rc *reconcile.Context, xstore *polardbxv1.XStore) (map[string]string, error) {
	config := xstore.Spec.Config

	templateVal, err := readValue(rc, config.Engine.Template)
	if err != nil {
		return nil, err
	}
	overrideVal, err := readValue(rc, config.Engine.Override)
	if err != nil {
		return nil, err
	}

	data := make(map[string]string)
	if len(templateVal) > 0 {
		data[convention.ConfigMyCnfTemplate] = fakeIniWithDefaultSectionIfNoSectionAtStart(templateVal, "mysqld")
	}

	if len(overrideVal) > 0 {
		overrideVal = fakeIniWithDefaultSectionIfNoSectionAtStart(overrideVal, "mysqld")
		data[convention.ConfigMyCnfOverride], err = syncParameterToConfigMap(rc, xstore, overrideVal)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

func NewConfigConfigMap(rc *reconcile.Context, xstore *polardbxv1.XStore) (*corev1.ConfigMap, error) {
	data, err := newConfigDataMap(rc, xstore)
	if err != nil {
		return nil, err
	}

	hashStr, err := security.HashObj(data)
	if err != nil {
		return nil, err
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewConfigMapName(xstore, convention.ConfigMapTypeConfig),
			Namespace: xstore.Namespace,
			Labels: k8shelper.PatchLabels(convention.ConstLabels(xstore), convention.LabelGeneration(xstore), map[string]string{
				xstoremeta.LabelHash: hashStr,
			}),
		},
		Immutable: pointer.Bool(false),
		Data:      data,
	}, nil
}

func NewTaskConfigMap(xstore *polardbxv1.XStore) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewConfigMapName(xstore, convention.ConfigMapTypeTask),
			Namespace: xstore.Namespace,
			Labels:    k8shelper.PatchLabels(convention.ConstLabels(xstore), convention.LabelGeneration(xstore)),
		},
		Immutable: pointer.Bool(false),
	}
}
