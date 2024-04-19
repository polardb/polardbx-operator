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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/factory"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/galaxy"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

var CreateOrUpdateConfigMaps = plugin.NewStepBinder(galaxy.Engine, "CreateOrUpdateConfigMaps",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		for _, cmType := range []convention.ConfigMapType{
			convention.ConfigMapTypeConfig,
			convention.ConfigMapTypeShared,
			convention.ConfigMapTypeTask,
			convention.ConfigMapTypeTde,
		} {
			loopFlow := flow.WithLoggerValues("configmap-type", cmType)
			if !rc.IsTdeEnable() && cmType == convention.ConfigMapTypeTde {
				continue
			}
			cm, err := rc.GetXStoreConfigMap(cmType)
			if client.IgnoreNotFound(err) != nil {
				return loopFlow.Error(err, "Unable to get configmap.")
			}

			// 1. Branch not found, create a new one.
			// 2. Branch found, but outdated (by comparing the generation label), update the configmap.
			if cm == nil {
				cm, err = factory.NewConfigMap(rc, xstore, cmType)
				if err != nil {
					return loopFlow.Error(err, "Unable to construct configmap.")
				}

				if err := rc.SetControllerRefAndCreate(cm); err != nil {
					return loopFlow.Error(err, "Unable to create configmap.")
				}
				loopFlow.Logger().Info("ConfigMap is created.")
			} else {
				outdated, err := convention.IsGenerationOutdated(xstore, cm)
				if err != nil {
					return loopFlow.Error(err, "Unable to resolve generation.")
				}

				if outdated {
					loopFlow.Logger().Info("ConfigMap is outdated, try update.")
					newCm, err := factory.NewConfigMap(rc, xstore, cmType)
					if err != nil {
						return loopFlow.Error(err, "Unable to construct configmap.")
					}
					err = rc.SetControllerRef(newCm)
					if err != nil {
						return loopFlow.Error(err, "Unable to set controller reference.")
					}
					if err := rc.Client().Update(rc.Context(), newCm); err != nil {
						return loopFlow.Error(err, "Unable to update configmap.")
					}
				}
			}
		}

		return flow.Pass()
	},
)
