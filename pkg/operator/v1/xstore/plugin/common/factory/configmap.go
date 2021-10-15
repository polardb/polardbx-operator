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
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/common/channel"
)

func NewSharedConfigMap(xstore *polardbxv1.XStore) (*corev1.ConfigMap, error) {
	sharedChannel := channel.SharedChannel{
		Status: channel.ChannelBlocked,
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewConfigMapName(xstore, convention.ConfigMapTypeShared),
			Namespace: xstore.Namespace,
			Labels:    convention.ConstLabels(xstore),
		},
		Immutable: pointer.Bool(false),
		Data: map[string]string{
			channel.SharedChannelKey: sharedChannel.String(),
		},
	}, nil
}
