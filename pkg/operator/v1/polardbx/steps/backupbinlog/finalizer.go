package backupbinlog

import (
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var AddFinalizer = polardbxv1reconcile.NewStepBinder("AddFinalizer", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	if controllerutil.ContainsFinalizer(backupBinlog, meta.Finalizer) {
		return flow.Pass()
	}
	controllerutil.AddFinalizer(backupBinlog, meta.Finalizer)
	rc.MarkPolarDBXChanged()
	return flow.Continue("Add finalizer.")
})

var RemoveFinalizer = polardbxv1reconcile.NewStepBinder("RemoveFinalizer", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	if !controllerutil.ContainsFinalizer(backupBinlog, meta.Finalizer) {
		return flow.Pass()
	}
	controllerutil.RemoveFinalizer(backupBinlog, meta.Finalizer)
	rc.MarkPolarDBXChanged()
	return flow.Continue("Remove finalizer.")
})
