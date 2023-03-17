package backupbinlog

import (
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var InitFromPxc = polardbxv1reconcile.NewStepBinder("InitFromPxc", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	pxc := rc.MustGetPolarDBX()
	pxc.SetAnnotations(k8shelper.PatchAnnotations(pxc.GetAnnotations(), map[string]string{
		meta.AnnotationBackupBinlog: "true",
	}))
	err := rc.Client().Update(rc.Context(), pxc)
	if err != nil {
		return flow.RetryErr(err, "failed to update pxc ", "pxc name", pxc.Name)
	}
	backupBinlog.Spec.PxcUid = string(pxc.UID)
	labels := backupBinlog.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels = k8shelper.PatchLabels(labels, map[string]string{
		meta.LabelName: backupBinlog.Spec.PxcName,
		meta.LabelUid:  backupBinlog.Spec.PxcUid,
	})
	backupBinlog.SetLabels(labels)
	rc.MarkPolarDBXChanged()
	return flow.Continue("InitFromPxc.")
})

var CleanFromPxc = polardbxv1reconcile.NewStepBinder("CleanFromPxc", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	pxc := rc.MustGetPolarDBX()
	annotation := pxc.GetAnnotations()
	if _, ok := annotation[meta.AnnotationBackupBinlog]; !ok {
		return flow.Pass()
	}
	delete(annotation, meta.AnnotationBackupBinlog)
	pxc.SetAnnotations(annotation)
	err := rc.Client().Update(rc.Context(), pxc)
	if err != nil {
		return flow.RetryErr(err, "failed to update pxc ", "pxc name", pxc.Name)
	}
	rc.MarkPolarDBXChanged()
	return flow.Continue("CleanFromPxc.")
})

func WhenPxcExist(binders ...control.BindFunc) control.BindFunc {
	return polardbxv1reconcile.NewStepIfBinder("PxcExist",
		func(rc *polardbxv1reconcile.Context, log logr.Logger) (bool, error) {
			backupBinlog := rc.MustGetPolarDBXBackupBinlog()
			polardbx, err := rc.GetPolarDBX()
			if apierrors.IsNotFound(err) || string(polardbx.UID) != backupBinlog.Spec.PxcUid {
				return false, nil
			}
			return true, nil
		},
		binders...,
	)
}
