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
