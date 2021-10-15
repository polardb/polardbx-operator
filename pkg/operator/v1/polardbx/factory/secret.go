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
	"k8s.io/apimachinery/pkg/util/rand"

	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
)

func (f *objectFactory) NewSecret() (*corev1.Secret, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}

	privileges := polardbx.Spec.Privileges

	// Random password if not specified.
	accounts := make(map[string]string)
	for _, priv := range privileges {
		if len(priv.Username) > 0 {
			password := priv.Password
			if len(password) == 0 {
				password = rand.String(8)
			}
			accounts[priv.Username] = password
		}
	}

	// If polardbx_root is not specified, generate for it.
	if _, ok := accounts[convention.RootAccount]; !ok {
		accounts[convention.RootAccount] = rand.String(8)
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewSecretName(polardbx, convention.SecretTypeAccount),
			Namespace: polardbx.Namespace,
		},
		StringData: accounts,
	}, nil
}

func (f *objectFactory) NewEncKeySecret() (*corev1.Secret, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewSecretName(polardbx, convention.SecretTypeSecurity),
			Namespace: polardbx.Namespace,
		},
		StringData: map[string]string{
			convention.SecretKeyEncodeKey: rand.String(16),
		},
	}, nil
}
