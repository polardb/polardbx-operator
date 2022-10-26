package instance

import (
	polarxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var CleanRebuildJob = xstorev1reconcile.NewStepBinder("CleanRebuildJob",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		var err error
		xstoreFollowerList := &polarxv1.XStoreFollowerList{}
		listOpts := []client.ListOption{
			client.InNamespace(rc.Namespace()),
			client.MatchingLabels{xstoremeta.LabelName: rc.Name()},
		}
		err = rc.Client().List(rc.Context(), xstoreFollowerList, listOpts...)
		if err != nil {
			return flow.RetryErr(err, "failed to list xstore followers")
		}
		if len(xstoreFollowerList.Items) == 0 {
			return flow.Continue("CleanRebuildJob success.")
		}
		xstoreFollower := &polarxv1.XStoreFollower{}
		opts := []client.DeleteAllOfOption{
			client.InNamespace(rc.Namespace()),
			client.MatchingLabels{xstoremeta.LabelName: rc.Name()},
		}
		err = rc.Client().DeleteAllOf(rc.Context(), xstoreFollower, opts...)
		if err != nil {
			return flow.RetryErr(err, "failed to delete all xstore followers")
		}
		return flow.Retry("CleanRebuildJob retry.")
	})
