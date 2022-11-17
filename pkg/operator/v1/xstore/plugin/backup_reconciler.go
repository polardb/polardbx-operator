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

package plugin

import (
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"sync"
)

var (
	xstoreBackupReconcilerMap = map[string]reconcile.BackupReconciler{}
	xstoreBackupReconcilerMu  sync.RWMutex
)

func RegisterXStoreBackupReconciler(engine string, reconciler reconcile.BackupReconciler) {
	xstoreBackupReconcilerMu.Lock()
	defer xstoreBackupReconcilerMu.Unlock()

	_, ok := xstoreBackupReconcilerMap[engine]
	if ok {
		panic("duplicate engine: " + engine)
	}
	xstoreBackupReconcilerMap[engine] = reconciler
}

func GetXStoreBackupReconciler(engine string) reconcile.BackupReconciler {
	xstoreBackupReconcilerMu.RLock()
	defer xstoreBackupReconcilerMu.RUnlock()

	return xstoreBackupReconcilerMap[engine]
}
