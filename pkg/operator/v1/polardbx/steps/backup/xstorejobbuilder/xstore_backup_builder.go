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

package xstorebackup

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
)

func NewXStoreBackup(scheme *runtime.Scheme, backup *polardbxv1.PolarDBXBackup, xstore *polardbxv1.XStore) (*polardbxv1.XStoreBackup, error) {

	xstoreBackup := &polardbxv1.XStoreBackup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: backup.Namespace,
			Name:      backup.Name + "-" + xstore.Name,
			Labels: map[string]string{
				meta.LabelName:            backup.Spec.Cluster.Name,
				meta.LabelTopBackup:       backup.Name,
				meta.LabelBackupXStore:    xstore.Name,
				meta.LabelBackupXStoreUID: string(xstore.UID),
			},
		},
		Spec: polardbxv1.XStoreBackupSpec{
			XStore: polardbxv1.XStoreReference{
				Name: xstore.Name,
			},
			RetentionTime:   backup.Spec.RetentionTime,
			StorageProvider: backup.Spec.StorageProvider,
		},
	}

	// set preferred backup node
	if node, ok := backup.Labels[meta.LabelPreferredBackupNode]; ok {
		xstoreBackup.Labels[meta.LabelPreferredBackupNode] = node
	}

	if err := ctrl.SetControllerReference(backup, xstoreBackup, scheme); err != nil {
		return nil, err
	}
	return xstoreBackup, nil
}
