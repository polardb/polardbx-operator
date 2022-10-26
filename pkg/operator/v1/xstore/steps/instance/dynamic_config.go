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
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/go-logr/logr"
)

func IsDynamicConfigChanged(rc *reconcile.Context, xStore *polardbxv1.XStore) (bool, error) {
	if xStore.Generation > xStore.Status.ObservedGeneration {
		if xStore.Spec.Config.Dynamic.LogDataSeparation != xStore.Status.ObservedConfig.Dynamic.LogDataSeparation {
			return true, nil
		}
		if xStore.Spec.Config.Dynamic.RpcProtocolVersion.String() !=
			xStore.Status.ObservedConfig.Dynamic.RpcProtocolVersion.String() {
			return true, nil
		}
	}
	return false, nil
}

func WhenDynamicConfigChanged(binders ...control.BindFunc) control.BindFunc {
	return reconcile.NewStepIfBinder("DynamicConfigChanged",
		func(rc *reconcile.Context, log logr.Logger) (bool, error) {
			return IsDynamicConfigChanged(rc, rc.MustGetXStore())
		},
		binders...,
	)
}
