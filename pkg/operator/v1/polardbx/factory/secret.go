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
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	"github.com/alibaba/polardbx-operator/pkg/util/defaults"
	"github.com/alibaba/polardbx-operator/pkg/util/ssl"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
)

func (f *objectFactory) NewSecretForRestore() (*corev1.Secret, error) {
	polardbx := f.rc.MustGetPolarDBX()

	originalSecret, err := f.rc.GetPolarDBXSecretForRestore()
	if err != nil || originalSecret == nil {
		return nil, err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewSecretName(polardbx, convention.SecretTypeAccount),
			Namespace: polardbx.Namespace,
			Labels:    convention.ConstLabels(polardbx),
		},
	}

	data := make(map[string][]byte)
	for user, passwd := range originalSecret.Data {
		data[user] = passwd
	}
	secret.Data = data
	return secret, nil
}

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
			Labels:    convention.ConstLabels(polardbx),
		},
		StringData: accounts,
	}, nil
}

func (f *objectFactory) NewSecuritySecret() (*corev1.Secret, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}
	stringData := map[string]string{}

	encodeKeySecretSelector := helper.GetEncodeKeySecretKeySelector(polardbx)
	if encodeKeySecretSelector != nil && len(encodeKeySecretSelector.Name) > 0 {
		secret, err := f.rc.GetSecret(encodeKeySecretSelector.Name)
		if err != nil {
			return nil, err
		}
		stringData[convention.SecretKeyEncodeKey] = string(secret.Data[encodeKeySecretSelector.Key])
	}

	// If not found, use the default one or random generate.
	if len(stringData[convention.SecretKeyEncodeKey]) == 0 {
		stringData[convention.SecretKeyEncodeKey] = defaults.NonEmptyStrOrDefault(
			f.rc.Config().Security().DefaultEncodeKey(), rand.String(16))
	}

	if helper.IsTLSEnabled(polardbx) {
		tls := polardbx.Spec.Security.TLS
		if tls.GenerateSelfSigned {
			caCert, caPriv, caPem, err := ssl.GenerateCA(2048, "")
			if err != nil {
				return nil, err
			}
			_, priv, pem, err := ssl.GenerateSelfSignedCert(caCert, caPriv, 2048, "")
			if err != nil {
				return nil, err
			}
			caPemBytes, err := ssl.MarshalCert(caPem)
			if err != nil {
				return nil, err
			}
			pemBytes, err := ssl.MarshalCert(pem)
			if err != nil {
				return nil, err
			}
			keyBytes, err := ssl.MarshalKey(priv)
			if err != nil {
				return nil, err
			}
			stringData[convention.SecretKeyRootCrt] = string(caPemBytes)
			stringData[convention.SecretKeyServerKey] = string(keyBytes)
			stringData[convention.SecretKeyServerCrt] = string(pemBytes)
		} else {
			// Copy from user-defined secret.
			secret, err := f.rc.GetSecret(tls.SecretName)
			if err != nil {
				return nil, err
			}
			stringData[convention.SecretKeyRootCrt] = string(secret.Data[convention.SecretKeyRootCrt])
			stringData[convention.SecretKeyServerKey] = string(secret.Data[convention.SecretKeyServerKey])
			stringData[convention.SecretKeyServerCrt] = string(secret.Data[convention.SecretKeyServerCrt])
		}
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewSecretName(polardbx, convention.SecretTypeSecurity),
			Namespace: polardbx.Namespace,
			Labels:    convention.ConstLabels(polardbx),
		},
		StringData: stringData,
	}, nil
}
