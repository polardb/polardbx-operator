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

func newConfigDataMap(rc *reconcile.Context, xstore *polardbxv1.XStore) (map[string]string, error) {
	config := xstore.Status.ObservedConfig

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
		data[convention.ConfigMyCnfTemplate] = templateVal
	}
	if len(overrideVal) > 0 {
		data[convention.ConfigMyCnfOverride] = overrideVal
	}
	return data, nil
}

func NewConfigConfigMap(rc *reconcile.Context, xstore *polardbxv1.XStore, engine string) (*corev1.ConfigMap, error) {
	data, err := newConfigDataMap(rc, xstore)
	if err != nil {
		return nil, err
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewConfigMapName(xstore, convention.ConfigMapTypeConfig),
			Namespace: xstore.Namespace,
			Labels:    k8shelper.PatchLabels(convention.ConstLabels(xstore), convention.LabelGeneration(xstore)),
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
			Labels:    convention.ConstLabels(xstore),
		},
		Immutable: pointer.Bool(false),
	}
}
