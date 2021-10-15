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

var CreateServices = plugin.NewStepBinder(galaxy.Engine, "CreateServices",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		for _, serviceType := range []convention.ServiceType{
			convention.ServiceTypeReadWrite,
			convention.ServiceTypeReadOnly,
			convention.ServiceTypeMetrics,
		} {
			svc, err := rc.GetXStoreService(serviceType)
			if client.IgnoreNotFound(err) != nil {
				return flow.Error(err, "Unable to get service.", "service-type", serviceType)
			}

			// Construct a new service if not found. Then create it via api server.
			if svc == nil {
				svc = factory.NewService(xstore, serviceType)
				err := rc.SetControllerRefAndCreate(svc)
				if err != nil {
					return flow.Error(err, "Unable to create service.", "service-type", serviceType)
				}
			}
		}

		return flow.Continue("Services all ready.")
	},
)
