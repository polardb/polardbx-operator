package backup

import (
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var AddFinalizer = NewStepBinder("AddFinalizer",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetXStoreBackup()
		if !controllerutil.ContainsFinalizer(backup, xstoremeta.Finalizer) {
			controllerutil.AddFinalizer(backup, xstoremeta.Finalizer)
			rc.MarkXstoreBackupChanged()
			return flow.Continue("Finalizer added.")
		}
		return flow.Pass()
	})

var RemoveFinalizer = NewStepBinder("RemoveFinalizer",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetXStoreBackup()
		if controllerutil.ContainsFinalizer(backup, xstoremeta.Finalizer) {
			controllerutil.RemoveFinalizer(backup, xstoremeta.Finalizer)
			rc.MarkXstoreBackupChanged()
			return flow.Continue("Finalizer removed.")
		}
		return flow.Pass()
	})
