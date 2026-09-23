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

package instance

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstorecommonfactory "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/factory"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/xcluster/xcluster"
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

var SyncEngineConfigMap = plugin.NewStepBinder(xcluster.Engine, "SyncEngineConfigMap",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (k8sreconcile.Result, error) {
		newConfigMap, err := xstorecommonfactory.NewConfigConfigMap(rc, rc.MustGetXStore())
		if err != nil {
			return flow.Error(err, "SyncEngineConfigMap Failed to newConfigMap", "engine", xcluster.Engine)
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
		rc.MarkXStoreChanged()
		return flow.Continue("finish SyncEngineConfigMap")
	},
)
