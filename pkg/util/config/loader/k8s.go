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

package loader

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bundle2 "github.com/alibaba/polardbx-operator/pkg/util/config/bundle"
)

type K8sOptions struct {
	Client client.Client
	Key    types.NamespacedName
}

type k8sLoader struct {
	client client.Client
	key    types.NamespacedName
}

func (l *k8sLoader) loadSecret(ctx context.Context) (*corev1.Secret, error) {
	var secret corev1.Secret
	err := l.client.Get(ctx, l.key, &secret)
	if err != nil {
		return nil, err
	}
	return &secret, nil
}

func (l *k8sLoader) loadConfigMap(ctx context.Context) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	err := l.client.Get(ctx, l.key, &cm)
	if err != nil {
		return nil, err
	}
	return &cm, nil
}

type k8sSecretLoader struct {
	k8sLoader
}

func (l *k8sSecretLoader) Load(ctx context.Context) (bundle2.Bundle, error) {
	secret, err := l.loadSecret(ctx)
	if err != nil {
		return nil, err
	}
	return bundle2.NewK8sSecretBundle(secret), nil
}

func NewK8sConfigMapLoader(opts K8sOptions) Loader {
	return &k8sSecretLoader{
		k8sLoader: k8sLoader{
			client: opts.Client,
			key:    opts.Key,
		},
	}
}

type k8sConfigMapLoader struct {
	k8sLoader
}

func (l *k8sConfigMapLoader) Load(ctx context.Context) (bundle2.Bundle, error) {
	cm, err := l.loadConfigMap(ctx)
	if err != nil {
		return nil, err
	}
	return bundle2.NewK8sConfigMapBundle(cm), nil
}

func NewK8sSecretLoader(opts K8sOptions) Loader {
	return &k8sConfigMapLoader{
		k8sLoader: k8sLoader{
			client: opts.Client,
			key:    opts.Key,
		},
	}
}
