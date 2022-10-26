package instance

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstorecommonfactory "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/factory"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/go-logr/logr"
	k8sreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func IsEngineConfigChanged(rc *reconcile.Context, xStore *polardbxv1.XStore) (bool, error) {
	newConfigMap, err := xstorecommonfactory.NewConfigConfigMap(rc, xStore)
	if err != nil {
		return false, err
	}
	oldConfigMap, err := rc.GetConfigMap(newConfigMap.Name)
	if err != nil {
		return false, err
	}
	newGeneration, err := xstoreconvention.GetGenerationLabelValue(newConfigMap)
	if err != nil {
		return false, err
	}
	oldGeneration, err := xstoreconvention.GetGenerationLabelValue(oldConfigMap)
	if err != nil {
		return false, err
	}
	if newGeneration > oldGeneration {
		newObjHash := xstoreconvention.GetHashLabelValue(newConfigMap)
		oldObjHash := xstoreconvention.GetHashLabelValue(oldConfigMap)
		if newObjHash != oldObjHash {
			return true, nil
		}
	}
	return false, nil
}

func WhenEngineConfigChanged(binders ...control.BindFunc) control.BindFunc {
	return reconcile.NewStepIfBinder("EngineConfigChanged",
		func(rc *reconcile.Context, log logr.Logger) (bool, error) {
			return IsEngineConfigChanged(rc, rc.MustGetXStore())
		},
		binders...,
	)
}

var SyncEngineConfigMap = xstorev1reconcile.NewStepBinder("SyncEngineConfigMap",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (k8sreconcile.Result, error) {
		newConfigMap, err := xstorecommonfactory.NewConfigConfigMap(rc, rc.MustGetXStore())
		if err != nil {
			return flow.Error(err, "SyncEngineConfigMap Failed to newConfigMap")
		}
		err = rc.SetControllerRef(newConfigMap)
		if err != nil {
			return flow.Error(err, "Unable to set controller reference.")
		}
		if err := rc.Client().Update(rc.Context(), newConfigMap); err != nil {
			return flow.Error(err, "Unable to update configmap.")
		}
		xstore := rc.MustGetXStore()
		xstore.Status.ObservedConfig.Engine.Override = xstore.Spec.Config.Engine.Override.DeepCopy()
		return flow.Continue("finish SyncEngineConfigMap")
	},
)
