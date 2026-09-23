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
	xstoreBackupBinlogReconcilerMap = map[string]reconcile.BackupBinlogReconciler{}
	xstoreBackupBinlogReconcilerMu  sync.RWMutex
)

func RegisterXStoreBackupBinlogReconciler(engine string, reconciler reconcile.BackupBinlogReconciler) {
	xstoreBackupBinlogReconcilerMu.Lock()
	defer xstoreBackupBinlogReconcilerMu.Unlock()

	_, ok := xstoreBackupBinlogReconcilerMap[engine]
	if ok {
		panic("duplicate engine: " + engine)
	}
	xstoreBackupBinlogReconcilerMap[engine] = reconciler
}

func GetXStoreBackupBinlogReconciler(engine string) reconcile.BackupBinlogReconciler {
	xstoreBackupBinlogReconcilerMu.RLock()
	defer xstoreBackupBinlogReconcilerMu.RUnlock()

	return xstoreBackupBinlogReconcilerMap[engine]
}
