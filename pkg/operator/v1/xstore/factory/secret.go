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
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/utils/pointer"
)

func NewSecret(xstore *polardbxv1.XStore) *corev1.Secret {
	data := make(map[string]string)
	privileges := xstore.Spec.Privileges
	if privileges != nil {
		for _, priv := range privileges {
			password := priv.Password
			if len(password) == 0 {
				password = rand.String(8)
			}
			data[priv.Username] = password
		}
	}

	// If super account not found, create one.
	if _, found := data[convention.SuperAccount]; !found {
		data[convention.SuperAccount] = rand.String(8)
	}

	// "root" is not allowed, delete it anyway.
	delete(data, "root")

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewSecretName(xstore),
			Namespace: xstore.Namespace,
			Labels:    convention.ConstLabels(xstore),
		},
		Immutable:  pointer.Bool(true),
		Type:       corev1.SecretTypeOpaque,
		StringData: data,
	}
}

func NewSecretForRestore(rc *reconcile.Context, xstore *polardbxv1.XStore) (*corev1.Secret, error) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewSecretName(xstore),
			Namespace: xstore.Namespace,
			Labels:    convention.ConstLabels(xstore),
		},
		Immutable: pointer.Bool(true),
		Type:      corev1.SecretTypeOpaque,
	}
	// try to get secret from pxb first
	var secretName string
	if xstore.Spec.Restore.BackupSet == "" || len(xstore.Spec.Restore.BackupSet) == 0 {
		backup, err := rc.GetLastCompletedXStoreBackup(map[string]string{
			xstoremeta.LabelName: xstore.Spec.Restore.From.XStoreName,
		}, rc.MustParseRestoreTime())
		if err != nil {
			return nil, err
		}
		secretName = backup.Name
	} else {
		secretName = xstore.Spec.Restore.BackupSet
	}
	xsbSecret, err := rc.GetSecretByName(secretName)
	if err != nil || xsbSecret == nil {
		return nil, err
	}
	secret.Data = xsbSecret.Data
	return secret, nil
}
