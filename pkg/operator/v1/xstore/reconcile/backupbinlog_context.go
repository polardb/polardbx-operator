package reconcile

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type BackupBinlogContext struct {
	*control.BaseReconcileContext
	xStoreContext                    *Context
	xstore                           *polardbxv1.XStore
	xstorePods                       []corev1.Pod
	xstoreTargetPod                  *corev1.Pod
	taskConfigMap                    *corev1.ConfigMap
	xstoreBackupBinlog               *polardbxv1.XStoreBackupBinlog
	xstoreBackupBinlogStatusSnapshot *polardbxv1.XStoreBackupBinlogStatus
	xstoreChanged                    bool
}

func NewBackupBinlogContext(base *control.BaseReconcileContext) *BackupBinlogContext {
	return &BackupBinlogContext{
		BaseReconcileContext: base,
	}
}

func (rc *BackupBinlogContext) MustGetXStoreBackupBinlog() *polardbxv1.XStoreBackupBinlog {
	xstoreBackupBinlog, err := rc.GetXStoreBackupBinlog()
	if err != nil {
		panic(err)
	}
	return xstoreBackupBinlog
}

func (rc *BackupBinlogContext) GetXStoreBackupBinlog() (*polardbxv1.XStoreBackupBinlog, error) {
	if rc.xstoreBackupBinlog == nil {
		var xstoreBackupBinlog polardbxv1.XStoreBackupBinlog
		err := rc.Client().Get(rc.Context(), rc.Request().NamespacedName, &xstoreBackupBinlog)
		if err != nil {
			return nil, err
		}
		rc.xstoreBackupBinlog = &xstoreBackupBinlog
		rc.xstoreBackupBinlogStatusSnapshot = rc.xstoreBackupBinlog.Status.DeepCopy()
	}
	return rc.xstoreBackupBinlog, nil
}

func (rc *BackupBinlogContext) MustGetXStore() *polardbxv1.XStore {
	xstore, err := rc.GetXStore()
	if err != nil {
		panic(err)
	}
	return xstore
}
func (rc *BackupBinlogContext) GetXStore() (*polardbxv1.XStore, error) {
	if rc.xstore == nil {
		backupbinlog := rc.MustGetXStoreBackupBinlog()
		var xstore polardbxv1.XStore
		xstoreSpec := types.NamespacedName{Namespace: rc.Request().Namespace, Name: backupbinlog.Spec.XStoreName}
		err := rc.Client().Get(rc.Context(), xstoreSpec, &xstore)
		if err != nil {
			return nil, err
		}
		rc.xstore = &xstore
	}
	return rc.xstore, nil
}

func (rc *BackupBinlogContext) GetActiveXStore() *polardbxv1.XStore {
	xstore, err := rc.GetXStore()
	if err != nil {
		panic(err)
	}
	if xstore.GetDeletionTimestamp().IsZero() {
		return xstore
	}
	return nil
}

func (rc *BackupBinlogContext) SetXStoreContext(xstoreContext *Context) {
	rc.xStoreContext = xstoreContext
}

func (rc *BackupBinlogContext) XStoreContext() *Context {
	return rc.xStoreContext
}

func (rc *BackupBinlogContext) MarkXStoreChanged() {
	rc.xstoreChanged = true
}

func (rc *BackupBinlogContext) IsXStoreChanged() bool {
	return rc.xstoreChanged
}

func (rc *BackupBinlogContext) UpdateXStoreBackupBinlog() error {
	backupBinlog := rc.MustGetXStoreBackupBinlog()
	err := rc.Client().Update(rc.Context(), backupBinlog)
	return err
}
